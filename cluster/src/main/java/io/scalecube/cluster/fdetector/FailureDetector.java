package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageHeaders;
import io.scalecube.transport.Transport;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FailureDetector implements IFailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);

  // qualifiers
  private static final String PING = "io.scalecube.cluster/fdetector/ping";
  private static final String PING_REQ = "io.scalecube.cluster/fdetector/pingReq";
  private static final String ACK = "io.scalecube.cluster/fdetector/ack";

  // filters
  private static final MessageHeaders.Filter ACK_FILTER = new MessageHeaders.Filter(ACK);
  private static final MessageHeaders.Filter PING_FILTER = new MessageHeaders.Filter(PING);
  private static final MessageHeaders.Filter PING_REQ_FILTER = new MessageHeaders.Filter(PING_REQ);
  private static final Func1<Message, Boolean> ORIGINAL_ISSUER_FILTER = new Func1<Message, Boolean>() {
    @Override
    public Boolean call(Message message) {
      return message.<PingData>data().getOriginalIssuer() != null;
    }
  };

  // Injected

  private final ITransport transport;
  private final FailureDetectorConfig config;

  // State

  private volatile List<Address> members = new ArrayList<>();
  private final AtomicInteger periodCounter = new AtomicInteger();
  private Set<Address> suspectedMembers = Sets.newConcurrentHashSet();

  // Subscriptions

  private Subscriber<Message> onPingRequestSubscriber;
  private Subscriber<Message> onAskToPingRequestSubscriber;
  private Subscriber<Message> onTransitAckRequestSubscriber;
  private Subject<FailureDetectorEvent, FailureDetectorEvent> subject =
      new SerializedSubject<>(PublishSubject.<FailureDetectorEvent>create());

  // Scheduled

  private final Scheduler scheduler;
  private ScheduledFuture<?> executorTask;
  private final ScheduledExecutorService executor;

  /**
   * Creates new instance of failure detector with given transport and default settings.
   *
   * @param transport transport
   */
  public FailureDetector(Transport transport) {
    this(transport, FailureDetectorConfig.DEFAULT);
  }

  /**
   * Creates new instance of failure detector with given transport and settings.
   *
   * @param transport transport
   * @param config failure detector settings
   */
  public FailureDetector(Transport transport, FailureDetectorConfig config) {
    checkArgument(transport != null);
    checkArgument(config != null);
    this.transport = transport;
    this.config = config;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("sc-failuredetector-%s").setDaemon(true).build());
    this.scheduler = Schedulers.from(executor);
  }

  @Override
  public void setMembers(Collection<Address> members) {
    Set<Address> set = new HashSet<>(members);
    set.remove(transport.address());
    List<Address> list = new ArrayList<>(set);
    Collections.shuffle(list);
    this.members = list;
    LOGGER.debug("Set cluster members[{}]: {}", this.members.size(), this.members);
  }

  public Collection<Address> getSuspectedMembers() {
    return Collections.unmodifiableCollection(suspectedMembers);
  }

  @Override
  public void start() {
    onPingRequestSubscriber = Subscribers.create(new OnPingRequestAction());
    transport.listen()
        .filter(PING_FILTER)
        .filter(targetFilter(transport.address()))
        .subscribe(onPingRequestSubscriber);

    onAskToPingRequestSubscriber = Subscribers.create(new OnAskToPingRequestAction());
    transport.listen()
        .filter(PING_REQ_FILTER)
        .subscribe(onAskToPingRequestSubscriber);

    onTransitAckRequestSubscriber = Subscribers.create(new OnTransitAckRequestAction());
    transport.listen()
        .filter(ACK_FILTER)
        .filter(ORIGINAL_ISSUER_FILTER)
        .subscribe(onTransitAckRequestSubscriber);

    executorTask = executor.scheduleWithFixedDelay(new FailureDetectorProtocolRunnable(), 0, config.getPingTime(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    // stop accepting requests
    if (onPingRequestSubscriber != null) {
      onPingRequestSubscriber.unsubscribe();
    }
    if (onAskToPingRequestSubscriber != null) {
      onAskToPingRequestSubscriber.unsubscribe();
    }
    if (onTransitAckRequestSubscriber != null) {
      onTransitAckRequestSubscriber.unsubscribe();
    }
    // cancel algorithm
    if (executorTask != null) {
      executorTask.cancel(true);
    }
    executor.shutdownNow(); // shutdown thread
    subject.onCompleted(); // stop publishing
  }

  @Override
  public Observable<FailureDetectorEvent> listenStatus() {
    return subject;
  }

  @Override
  public void suspect(Address member) {
    checkNotNull(member);
    if (suspectedMembers.add(member)) {
      LOGGER.debug("Member {} marked as SUSPECTED for {}", member, transport.address());
    }
  }

  @Override
  public void trust(Address member) {
    checkNotNull(member);
    if (suspectedMembers.remove(member)) {
      LOGGER.debug("Member {} marked as TRUSTED for {}", member, transport.address());
    }
  }

  private void doPing() {
    final Address pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    final Address localAddress = transport.address();
    final String period = Integer.toString(periodCounter.incrementAndGet());
    PingData pingData = new PingData(localAddress, pingMember);
    Message pingMsg = Message.withData(pingData).qualifier(PING).correlationId(period).build();
    LOGGER.trace("Send Ping from {} to {}", localAddress, pingMember);

    transport.listen()
        .filter(ackFilter(period))
        .filter(new CorrelationFilter(localAddress, pingMember))
        .take(1)
        .timeout(config.getPingTimeout(), TimeUnit.MILLISECONDS, scheduler)
        .subscribe(Subscribers.create(new Action1<Message>() {
          @Override
          public void call(Message transportMessage) {
            LOGGER.trace("Received PingAck from {}", pingMember);
            declareTrusted(pingMember);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            LOGGER.trace("No PingAck from {} within {}ms; about to make PingReq now",
                pingMember, config.getPingTimeout());
            doPingReq(pingMember, period);
          }
        }));

    transport.send(pingMember, pingMsg);
  }

  private void doPingReq(final Address pingMember, String period) {
    final int timeout = config.getPingTime() - config.getPingTimeout();
    if (timeout <= 0) {
      LOGGER.trace("No PingReq occurred, because no time left (pingTime={}, pingTimeout={})",
          config.getPingTime(), config.getPingTimeout());
      declareSuspected(pingMember);
      return;
    }

    final List<Address> pingReqMembers = selectPingReqMembers(pingMember);
    if (pingReqMembers.isEmpty()) {
      LOGGER.trace("No PingReq occurred, because member selection is empty");
      declareSuspected(pingMember);
      return;
    }

    Address localAddress = transport.address();
    transport.listen()
        .filter(ackFilter(period))
        .filter(new CorrelationFilter(localAddress, pingMember))
        .take(1)
        .timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(Subscribers.create(new Action1<Message>() {
          @Override
          public void call(Message message) {
            LOGGER.trace("PingReq OK (pinger={}, target={})", message.sender(), pingMember);
            declareTrusted(pingMember);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            LOGGER.trace("No PingAck on PingReq within {}ms (pingers={}, target={})", pingReqMembers, pingMember,
                timeout);
            declareSuspected(pingMember);
          }
        }));

    PingData pingReqData = new PingData(localAddress, pingMember);
    Message pingReqMsg = Message.withData(pingReqData).qualifier(PING_REQ).correlationId(period).build();
    for (Address pingReqMember : pingReqMembers) {
      LOGGER.trace("Send PingReq from {} to {}", localAddress, pingReqMember);
      transport.send(pingReqMember, pingReqMsg);
    }
  }

  /**
   * Adds given member to {@link #suspectedMembers} and emitting state {@code SUSPECTED}.
   */
  private void declareSuspected(Address member) {
    if (suspectedMembers.add(member)) {
      LOGGER.debug("Member {} detected as SUSPECTED by {}", member, transport.address());
      subject.onNext(new FailureDetectorEvent(member, ClusterMemberStatus.SUSPECTED));
    }
  }

  /**
   * Removes given member from {@link #suspectedMembers} and emitting state {@code TRUSTED}.
   */
  private void declareTrusted(Address member) {
    if (suspectedMembers.remove(member)) {
      LOGGER.debug("Member {} detected as TRUSTED by {}", member, transport.address());
      subject.onNext(new FailureDetectorEvent(member, ClusterMemberStatus.TRUSTED));
    }
  }

  private Address selectPingMember() {
    // TODO [AK]: Select ping member not randomly, but sequentially with shuffle after last member selected
    return !members.isEmpty() ? members.get(ThreadLocalRandom.current().nextInt(members.size())) : null;
  }

  private List<Address> selectPingReqMembers(Address pingMember) {
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
    return new MessageHeaders.Filter(ACK, correlationId);
  }

  private Func1<Message, Boolean> targetFilter(final Address address) {
    return new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        return message.<PingData>data().getTo().equals(address);
      }
    };
  }

  private static class CorrelationFilter implements Func1<Message, Boolean> {
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

  /**
   * Listens to PING message and answers with ACK.
   */
  private class OnPingRequestAction implements Action1<Message> {
    @Override
    public void call(Message message) {
      LOGGER.trace("Received Ping: {}", message);
      PingData data = message.data();
      String correlationId = message.correlationId();
      Message ackMessage = Message.withData(data).qualifier(ACK).correlationId(correlationId).build();
      transport.send(data.getFrom(), ackMessage);
    }
  }

  /**
   * Listens to PING_REQ message and sends PING to requested cluster member.
   */
  private class OnAskToPingRequestAction implements Action1<Message> {
    @Override
    public void call(Message message) {
      LOGGER.trace("Received PingReq: {}", message);
      PingData data = message.data();
      Address target = data.getTo();
      Address originalIssuer = data.getFrom();
      String correlationId = message.correlationId();
      PingData pingReqData = new PingData(transport.address(), target, originalIssuer);
      Message pingMessage = Message.withData(pingReqData).qualifier(PING).correlationId(correlationId).build();
      transport.send(target, pingMessage);
    }
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and sends it to
   * ORIGINAL_ISSUER.
   */
  private class OnTransitAckRequestAction implements Action1<Message> {
    @Override
    public void call(Message message) {
      PingData data = message.data();
      Address target = data.getOriginalIssuer();
      String correlationId = message.correlationId();
      PingData originalAckData = new PingData(target, data.getTo());
      Message originalAckMessage =
          Message.withData(originalAckData).qualifier(ACK).correlationId(correlationId).build();
      transport.send(target, originalAckMessage);
    }
  }

  private class FailureDetectorProtocolRunnable implements Runnable {
    @Override
    public void run() {
      try {
        doPing();
      } catch (Exception cause) {
        LOGGER.error("Unhandled exception: {}", cause, cause);
      }
    }
  }
}
