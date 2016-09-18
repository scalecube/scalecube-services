package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.membership.IMembershipProtocol;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.cluster.membership.MembershipEvent;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
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
  private final IMembershipProtocol membership;
  private final FailureDetectorConfig config;

  // State

  private long period = 0;
  private List<Member> pingMembers = new ArrayList<>();
  private int pingMemberIndex = 0; // index for sequential ping member selection

  // Subscriptions

  private Subscriber<Member> onMemberAddedSubscriber;
  private Subscriber<Member> onMemberRemovedSubscriber;
  private Subscriber<Message> onPingRequestSubscriber;
  private Subscriber<Message> onAskToPingRequestSubscriber;
  private Subscriber<Message> onTransitAckRequestSubscriber;

  // Subject

  private Subject<FailureDetectorEvent, FailureDetectorEvent> subject =
      PublishSubject.<FailureDetectorEvent>create().toSerialized();

  // Scheduled

  private final ScheduledExecutorService executor;
  private final Scheduler scheduler;
  private ScheduledFuture<?> pingTask;

  /**
   * Creates new instance of failure detector with given transport and settings.
   *
   * @param transport transport
   * @param config failure detector settings
   */
  public FailureDetector(ITransport transport, IMembershipProtocol membership, FailureDetectorConfig config) {
    checkArgument(transport != null);
    checkArgument(membership != null);
    checkArgument(config != null);
    this.transport = transport;
    this.membership = membership;
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
  public void start() {
    onMemberAddedSubscriber = Subscribers.create(this::onMemberAdded);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isAdded)
        .map(MembershipEvent::member)
        .subscribe(onMemberAddedSubscriber);

    onMemberRemovedSubscriber = Subscribers.create(pingMembers::remove);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isRemoved)
        .map(MembershipEvent::member)
        .subscribe(onMemberRemovedSubscriber);

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

  private void onMemberAdded(Member member) {
    // insert member into random positions
    int size = pingMembers.size();
    int index = size > 0 ? ThreadLocalRandom.current().nextInt(size) : 0;
    pingMembers.add(index, member);
  }

  @Override
  public void stop() {
    // Stop accepting requests
    if (onMemberAddedSubscriber != null) {
      onMemberAddedSubscriber.unsubscribe();
    }
    if (onMemberRemovedSubscriber != null) {
      onMemberRemovedSubscriber.unsubscribe();
    }
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

    final Member pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    // start algorithm
    try {
      final Member localMember = membership.member();
      final String cid = Long.toString(period);
      PingData pingData = new PingData(localMember, pingMember);
      Message pingMsg = Message.withData(pingData).qualifier(PING).correlationId(cid).build();
      LOGGER.trace("Send Ping from {} to {}", localMember, pingMember);

      transport.listen().observeOn(scheduler)
          .filter(ackFilter(cid))
          .filter(new CorrelationFilter(localMember, pingMember))
          .take(1)
          .timeout(config.getPingTimeout(), TimeUnit.MILLISECONDS, scheduler)
          .subscribe(message -> {
              LOGGER.trace("Received PingAck from {}", pingMember);
              declareStatus(pingMember, MemberStatus.ALIVE);
            }, throwable -> {
              LOGGER.trace("No PingAck from {} within {}ms; about to make PingReq now",
                  pingMember, config.getPingTimeout());
              doPingReq(pingMember, cid);
            });

      transport.send(pingMember.address(), pingMsg);
    } catch (Exception cause) {
      LOGGER.error("Unhandled exception: {}", cause, cause);
    }
  }

  private Member selectPingMember() {
    if (pingMembers.isEmpty()) {
      return null;
    }
    if (pingMemberIndex >= pingMembers.size()) {
      pingMemberIndex = 0;
      Collections.shuffle(pingMembers);
    }
    return pingMembers.get(pingMemberIndex++);
  }

  private void doPingReq(final Member pingMember, String cid) {
    final int timeout = config.getPingInterval() - config.getPingTimeout();
    if (timeout <= 0) {
      LOGGER.trace("No PingReq occurred, because no time left (pingTime={}, pingTimeout={})",
          config.getPingInterval(), config.getPingTimeout());
      declareStatus(pingMember, MemberStatus.SUSPECT);
      return;
    }

    final List<Member> pingReqMembers = selectPingReqMembers(pingMember);
    if (pingReqMembers.isEmpty()) {
      LOGGER.trace("No PingReq occurred, because member selection is empty");
      declareStatus(pingMember, MemberStatus.SUSPECT);
      return;
    }

    Member localMember = membership.member();
    transport.listen().observeOn(scheduler)
        .filter(ackFilter(cid))
        .filter(new CorrelationFilter(localMember, pingMember))
        .take(1)
        .timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(message -> {
            LOGGER.trace("PingReq OK (pinger={}, target={})", message.sender(), pingMember);
            declareStatus(pingMember, MemberStatus.ALIVE);
          }, throwable -> {
            LOGGER.trace("No PingAck on PingReq within {}ms (pingers={}, target={})",
                pingReqMembers, pingMember, timeout);
            declareStatus(pingMember, MemberStatus.SUSPECT);
          });

    PingData pingReqData = new PingData(localMember, pingMember);
    Message pingReqMsg = Message.withData(pingReqData).qualifier(PING_REQ).correlationId(cid).build();
    for (Member pingReqMember : pingReqMembers) {
      LOGGER.trace("Send PingReq from {} to {}", localMember, pingReqMember);
      transport.send(pingReqMember.address(), pingReqMsg);
    }
  }

  private void declareStatus(Member member, MemberStatus status) {
    LOGGER.debug("Member {} detected as {}", member, status);
    subject.onNext(new FailureDetectorEvent(member, status));
  }

  private List<Member> selectPingReqMembers(Member pingMember) {
    if (config.getPingReqMembers() <= 0) {
      return Collections.emptyList();
    }
    List<Member> candidates = new ArrayList<>(pingMembers);
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
    if (!data.getTo().equals(membership.member())) {
      LOGGER.warn("Received Ping to {}, but local member is {}", data.getTo(), transport.address());
      return;
    }
    String correlationId = message.correlationId();
    Message ackMessage = Message.withData(data).qualifier(PING_ACK).correlationId(correlationId).build();
    transport.send(data.getFrom().address(), ackMessage);
  }

  /**
   * Listens to PING_REQ message and sends PING to requested cluster member.
   */
  private void onPingReq(Message message) {
    LOGGER.trace("Received PingReq: {}", message);
    PingData data = message.data();
    Member target = data.getTo();
    Member originalIssuer = data.getFrom();
    String correlationId = message.correlationId();
    PingData pingReqData = new PingData(membership.member(), target, originalIssuer);
    Message pingMessage = Message.withData(pingReqData).qualifier(PING).correlationId(correlationId).build();
    transport.send(target.address(), pingMessage);
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and sends it to
   * ORIGINAL_ISSUER.
   */
  private void onTransitAckResponse(Message message) {
    PingData data = message.data();
    Member target = data.getOriginalIssuer();
    String correlationId = message.correlationId();
    PingData originalAckData = new PingData(target, data.getTo());
    Message originalAckMessage = Message.withData(originalAckData).qualifier(PING_ACK).correlationId(correlationId).build();
    transport.send(target.address(), originalAckMessage);
  }

  private static final class CorrelationFilter implements Func1<Message, Boolean> {
    final Member from;
    final Member target;

    CorrelationFilter(Member from, Member target) {
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
