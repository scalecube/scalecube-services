package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageHeaders;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class FailureDetector implements IFailureDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);

  // qualifiers
  public static final String PING = "sc/fdetector/ping";
  public static final String PING_REQ = "sc/fdetector/pingReq";
  public static final String PING_ACK = "sc/fdetector/pingAck";

  // filters
  private static final MessageHeaders.Filter ACK_FILTER = new MessageHeaders.Filter(PING_ACK);
  private static final MessageHeaders.Filter PING_FILTER = new MessageHeaders.Filter(PING);
  private static final MessageHeaders.Filter PING_REQ_FILTER = new MessageHeaders.Filter(PING_REQ);

  private static final Func1<Message, Boolean> ORIGINAL_ISSUER_FILTER =
      message -> message.<PingData>data().getOriginalIssuer() != null;

  // Injected

  private final ITransport transport;
  private final FailureDetectorConfig config;

  // State

  private long period = 0;
  private int pingMemberIndex = 0; // index for sequential ping member selection
  private List<Address> members = new ArrayList<>();

  // Subscriptions

  private Subscriber<Message> onPingRequestSubscriber;
  private Subscriber<Message> onAskToPingRequestSubscriber;
  private Subscriber<Message> onTransitAckRequestSubscriber;
  private Subject<FailureDetectorEvent, FailureDetectorEvent> subject =
      PublishSubject.<FailureDetectorEvent>create().toSerialized();

  // Scheduled

  private final Scheduler scheduler;
  private ScheduledFuture<?> pingTask;
  private final ScheduledExecutorService executor;

  /**
   * Creates new instance of failure detector with given transport and default settings.
   *
   * @param transport transport
   */
  public FailureDetector(ITransport transport) {
    this(transport, FailureDetectorConfig.defaultConfig());
  }

  /**
   * Creates new instance of failure detector with given transport and settings.
   *
   * @param transport transport
   * @param config failure detector settings
   */
  public FailureDetector(ITransport transport, FailureDetectorConfig config) {
    checkArgument(transport != null);
    checkArgument(config != null);
    this.transport = transport;
    this.config = config;
    String nameFormat = "sc-fdetector-" + transport.address().toString();
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
    this.scheduler = Schedulers.from(executor);
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  ITransport getTransport() {
    return transport;
  }

  @Override
  public void setMembers(Collection<Address> members) {
    Set<Address> set = new HashSet<>(members);
    set.remove(transport.address());
    List<Address> list = new ArrayList<>(set);
    Collections.shuffle(list);
    executor.execute(() -> {
        pingMemberIndex = 0;
        this.members = list;
        LOGGER.debug("Updated monitored members[{}]: {}", this.members.size(), this.members);
      });
  }

  @Override
  public void start() {
    onPingRequestSubscriber = Subscribers.create(this::onPing);
    transport.listen().observeOn(scheduler)
        .filter(PING_FILTER)
        .subscribe(onPingRequestSubscriber);

    onAskToPingRequestSubscriber = Subscribers.create(this::onPingReq);
    transport.listen().observeOn(scheduler)
        .filter(PING_REQ_FILTER)
        .subscribe(onAskToPingRequestSubscriber);

    onTransitAckRequestSubscriber = Subscribers.create(this::onTransitAckResponse);
    transport.listen().observeOn(scheduler)
        .filter(ACK_FILTER)
        .filter(ORIGINAL_ISSUER_FILTER)
        .subscribe(onTransitAckRequestSubscriber);

    pingTask = executor.scheduleWithFixedDelay(
        this::doPing, config.getPingInterval(), config.getPingInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    // Stop accepting requests
    if (onPingRequestSubscriber != null) {
      onPingRequestSubscriber.unsubscribe();
    }
    if (onAskToPingRequestSubscriber != null) {
      onAskToPingRequestSubscriber.unsubscribe();
    }
    if (onTransitAckRequestSubscriber != null) {
      onTransitAckRequestSubscriber.unsubscribe();
    }

    // Stop sending pings
    if (pingTask != null) {
      pingTask.cancel(true);
    }

    // Shutdown executor
    executor.shutdown();

    // Stop publishing events
    subject.onCompleted();
  }

  @Override
  public Observable<FailureDetectorEvent> listen() {
    return subject.toSerialized();
  }

  private void doPing() {
    period++;

    final Address pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    // start algorithm
    try {
      final Address localAddress = transport.address();
      final String cid = Long.toString(period);
      PingData pingData = new PingData(localAddress, pingMember);
      Message pingMsg = Message.withData(pingData).qualifier(PING).correlationId(cid).build();
      LOGGER.trace("Send Ping from {} to {}", localAddress, pingMember);

      transport.listen().observeOn(scheduler)
          .filter(ackFilter(cid))
          .filter(new CorrelationFilter(localAddress, pingMember))
          .take(1)
          .timeout(config.getPingTimeout(), TimeUnit.MILLISECONDS, scheduler)
          .subscribe(message -> {
              LOGGER.trace("Received PingAck from {}", pingMember);
              declareTrusted(pingMember);
            }, throwable -> {
              LOGGER.trace("No PingAck from {} within {}ms; about to make PingReq now",
                  pingMember, config.getPingTimeout());
              doPingReq(pingMember, cid);
            });

      transport.send(pingMember, pingMsg);
    } catch (Exception cause) {
      LOGGER.error("Unhandled exception: {}", cause, cause);
    }
  }

  private Address selectPingMember() {
    if (members.isEmpty()) {
      return null;
    }
    if (pingMemberIndex >= members.size()) {
      pingMemberIndex = 0;
      Collections.shuffle(members);
    }
    return members.get(pingMemberIndex++);
  }

  private void doPingReq(final Address pingMember, String cid) {
    final int timeout = config.getPingInterval() - config.getPingTimeout();
    if (timeout <= 0) {
      LOGGER.trace("No PingReq occurred, because no time left (pingTime={}, pingTimeout={})",
          config.getPingInterval(), config.getPingTimeout());
      declareSuspected(pingMember);
      return;
    }

    final List<Address> pingReqMembers = selectPingReqMembers(pingMember, members);
    if (pingReqMembers.isEmpty()) {
      LOGGER.trace("No PingReq occurred, because member selection is empty");
      declareSuspected(pingMember);
      return;
    }

    Address localAddress = transport.address();
    transport.listen().observeOn(scheduler)
        .filter(ackFilter(cid))
        .filter(new CorrelationFilter(localAddress, pingMember))
        .take(1)
        .timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(message -> {
            LOGGER.trace("PingReq OK (pinger={}, target={})", message.sender(), pingMember);
            declareTrusted(pingMember);
          }, throwable -> {
            LOGGER.trace("No PingAck on PingReq within {}ms (pingers={}, target={})",
                pingReqMembers, pingMember, timeout);
            declareSuspected(pingMember);
          });

    PingData pingReqData = new PingData(localAddress, pingMember);
    Message pingReqMsg = Message.withData(pingReqData).qualifier(PING_REQ).correlationId(cid).build();
    for (Address pingReqMember : pingReqMembers) {
      LOGGER.trace("Send PingReq from {} to {}", localAddress, pingReqMember);
      transport.send(pingReqMember, pingReqMsg);
    }
  }

  private void declareSuspected(Address member) {
    LOGGER.debug("Member {} detected as SUSPECTED by {}", member, transport.address());
    subject.onNext(new FailureDetectorEvent(member, MemberStatus.SUSPECT));
  }

  private void declareTrusted(Address member) {
    LOGGER.debug("Member {} detected as TRUSTED by {}", member, transport.address());
    subject.onNext(new FailureDetectorEvent(member, MemberStatus.ALIVE));
  }

  private List<Address> selectPingReqMembers(Address pingMember, List<Address> members) {
    if (config.getPingReqMembers() <= 0) {
      return Collections.emptyList();
    }
    List<Address> candidates = new ArrayList<>(members);
    candidates.remove(pingMember);
    if (candidates.isEmpty()) {
      return Collections.emptyList();
    }
    Collections.shuffle(candidates);
    boolean selectAll = candidates.size() < config.getPingReqMembers();
    return selectAll ? candidates : candidates.subList(0, config.getPingReqMembers());
  }

  private Func1<Message, Boolean> ackFilter(String correlationId) {
    return new MessageHeaders.Filter(PING_ACK, correlationId);
  }

  /**
   * Listens to PING message and answers with ACK.
   */
  private void onPing(Message message) {
    LOGGER.trace("Received Ping: {}", message);
    PingData data = message.data();
    if (!data.getTo().equals(transport.address())) {
      LOGGER.warn("Received Ping to {}, but local address is {}", data.getTo(), transport.address());
      return;
    }
    String correlationId = message.correlationId();
    Message ackMessage = Message.withData(data).qualifier(PING_ACK).correlationId(correlationId).build();
    transport.send(data.getFrom(), ackMessage);
  }

  /**
   * Listens to PING_REQ message and sends PING to requested cluster member.
   */
  private void onPingReq(Message message) {
    LOGGER.trace("Received PingReq: {}", message);
    PingData data = message.data();
    Address target = data.getTo();
    Address originalIssuer = data.getFrom();
    String correlationId = message.correlationId();
    PingData pingReqData = new PingData(transport.address(), target, originalIssuer);
    Message pingMessage = Message.withData(pingReqData).qualifier(PING).correlationId(correlationId).build();
    transport.send(target, pingMessage);
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and sends it to
   * ORIGINAL_ISSUER.
   */
  private void onTransitAckResponse(Message message) {
    PingData data = message.data();
    Address target = data.getOriginalIssuer();
    String correlationId = message.correlationId();
    PingData originalAckData = new PingData(target, data.getTo());
    Message originalAckMessage = Message.withData(originalAckData).qualifier(PING_ACK).correlationId(correlationId).build();
    transport.send(target, originalAckMessage);
  }

  private static final class CorrelationFilter implements Func1<Message, Boolean> {
    final Address from;
    final Address target;

    CorrelationFilter(Address from, Address target) {
      this.from = from;
      this.target = target;
    }

    @Override
    public Boolean call(Message message) {
      PingData data = message.data();
      return from.equals(data.getFrom()) && target.equals(data.getTo()) && data.getOriginalIssuer() == null;
    }
  }
}
