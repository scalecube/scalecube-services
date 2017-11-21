package io.scalecube.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipProtocol;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
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

public final class FailureDetectorImpl implements FailureDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorImpl.class);

  // Qualifiers

  public static final String PING = "sc/fdetector/ping";
  public static final String PING_REQ = "sc/fdetector/pingReq";
  public static final String PING_ACK = "sc/fdetector/pingAck";

  // Injected

  private final Transport transport;
  private final MembershipProtocol membership;
  private final FailureDetectorConfig config;

  // State

  private long period = 0;
  private List<Member> pingMembers = new ArrayList<>();
  private int pingMemberIndex = 0; // index for sequential ping member selection

  // Subscriptions

  private Subscriber<Member> onMemberAddedSubscriber;
  private Subscriber<Member> onMemberRemovedSubscriber;
  private Subscriber<MembershipEvent> onMemberUpdatedSubscriber;
  private Subscriber<Message> onPingRequestSubscriber;
  private Subscriber<Message> onAskToPingRequestSubscriber;
  private Subscriber<Message> onTransitPingAckRequestSubscriber;

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
   * @param membership membership protocol
   * @param config failure detector settings
   */
  public FailureDetectorImpl(Transport transport, MembershipProtocol membership, FailureDetectorConfig config) {
    checkArgument(transport != null);
    checkArgument(membership != null);
    checkArgument(config != null);
    this.transport = transport;
    this.membership = membership;
    this.config = config;
    String nameFormat = "sc-fdetector-" + Integer.toString(membership.member().address().port());
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
    this.scheduler = Schedulers.from(executor);
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  Transport getTransport() {
    return transport;
  }

  @Override
  public void start() {
    onMemberAddedSubscriber = Subscribers.create(this::onMemberAdded, this::onError);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isAdded)
        .map(MembershipEvent::member)
        .subscribe(onMemberAddedSubscriber);

    onMemberRemovedSubscriber = Subscribers.create(this::onMemberRemoved, this::onError);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isRemoved)
        .map(MembershipEvent::member)
        .subscribe(onMemberRemovedSubscriber);

    onMemberUpdatedSubscriber = Subscribers.create(this::onMemberUpdated, this::onError);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isUpdated)
        .subscribe(onMemberUpdatedSubscriber);

    onPingRequestSubscriber = Subscribers.create(this::onPing, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(this::isPing)
        .subscribe(onPingRequestSubscriber);

    onAskToPingRequestSubscriber = Subscribers.create(this::onPingReq, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(this::isPingReq)
        .subscribe(onAskToPingRequestSubscriber);

    onTransitPingAckRequestSubscriber = Subscribers.create(this::onTransitPingAck, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(this::isTransitPingAck)
        .subscribe(onTransitPingAckRequestSubscriber);

    pingTask = executor.scheduleWithFixedDelay(
        this::doPing, config.getPingInterval(), config.getPingInterval(), TimeUnit.MILLISECONDS);
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
    if (onMemberUpdatedSubscriber != null) {
      onMemberUpdatedSubscriber.unsubscribe();
    }
    if (onPingRequestSubscriber != null) {
      onPingRequestSubscriber.unsubscribe();
    }
    if (onAskToPingRequestSubscriber != null) {
      onAskToPingRequestSubscriber.unsubscribe();
    }
    if (onTransitPingAckRequestSubscriber != null) {
      onTransitPingAckRequestSubscriber.unsubscribe();
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
    return subject.onBackpressureDrop().asObservable();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doPing() {
    // Increment period counter
    period++;

    // Select ping member
    Member pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    // Send ping
    Member localMember = membership.member();
    String cid = localMember.id() + "-" + Long.toString(period);
    PingData pingData = new PingData(localMember, pingMember);
    Message pingMsg = Message.withData(pingData).qualifier(PING).correlationId(cid).build();
    try {
      LOGGER.trace("Send Ping[{}] to {}", period, pingMember);
      transport.listen().observeOn(scheduler)
          .filter(this::isPingAck)
          .filter(message -> cid.equals(message.correlationId()))
          .take(1)
          .timeout(config.getPingTimeout(), TimeUnit.MILLISECONDS, scheduler)
          .subscribe(
              message -> {
                LOGGER.trace("Received PingAck[{}] from {}", period, pingMember);
                publishPingResult(pingMember, MemberStatus.ALIVE);
              },
              throwable -> {
                LOGGER.trace("Timeout getting PingAck[{}] from {} within {} ms",
                    period, pingMember, config.getPingTimeout());
                doPingReq(pingMember, cid);
              });
      transport.send(pingMember.address(), pingMsg);
    } catch (Exception cause) {
      LOGGER.error("Exception on sending Ping[{}] to {}: {}", period, pingMember, cause.getMessage(), cause);
    }
  }

  private void doPingReq(final Member pingMember, String cid) {
    final int timeout = config.getPingInterval() - config.getPingTimeout();
    if (timeout <= 0) {
      LOGGER.trace("No PingReq[{}] occurred, because no time left (pingInterval={}, pingTimeout={})",
          period, config.getPingInterval(), config.getPingTimeout());
      publishPingResult(pingMember, MemberStatus.SUSPECT);
      return;
    }

    final List<Member> pingReqMembers = selectPingReqMembers(pingMember);
    if (pingReqMembers.isEmpty()) {
      LOGGER.trace("No PingReq[{}] occurred, because member selection is empty", period);
      publishPingResult(pingMember, MemberStatus.SUSPECT);
      return;
    }

    Member localMember = membership.member();
    transport.listen().observeOn(scheduler)
        .filter(this::isPingAck)
        .filter(message -> cid.equals(message.correlationId()))
        .take(1)
        .timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
        .subscribe(
            message -> {
              LOGGER.trace("Received transit PingAck[{}] from {} to {}", period, message.sender(), pingMember);
              publishPingResult(pingMember, MemberStatus.ALIVE);
            },
            throwable -> {
              LOGGER.trace("Timeout getting transit PingAck[{}] from {} to {} within {} ms",
                  period, pingReqMembers, pingMember, timeout);
              publishPingResult(pingMember, MemberStatus.SUSPECT);
            });

    PingData pingReqData = new PingData(localMember, pingMember);
    Message pingReqMsg = Message.withData(pingReqData).qualifier(PING_REQ).correlationId(cid).build();
    LOGGER.trace("Send PingReq[{}] to {} for {}", period, pingReqMembers, pingMember);
    for (Member pingReqMember : pingReqMembers) {
      transport.send(pingReqMember.address(), pingReqMsg);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private void onMemberAdded(Member member) {
    // insert member into random positions
    int size = pingMembers.size();
    int index = size > 0 ? ThreadLocalRandom.current().nextInt(size) : 0;
    pingMembers.add(index, member);
  }

  private void onMemberRemoved(Member member) {
    pingMembers.remove(member);
  }

  private void onMemberUpdated(MembershipEvent membershipEvent) {
    int index = pingMembers.indexOf(membershipEvent.oldMember());
    if (index != -1) { // except local
      pingMembers.set(index, membershipEvent.newMember());
    }
  }

  /**
   * Listens to PING message and answers with ACK.
   */
  private void onPing(Message message) {
    LOGGER.trace("Received Ping: {}", message);
    PingData data = message.data();
    Member localMember = membership.member();
    if (!data.getTo().id().equals(localMember.id())) {
      LOGGER.warn("Received Ping to {}, but local member is {}", data.getTo(), localMember);
      return;
    }
    String correlationId = message.correlationId();
    Message ackMessage = Message.withData(data).qualifier(PING_ACK).correlationId(correlationId).build();
    LOGGER.trace("Send PingAck to {}", data.getFrom().address());
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
    LOGGER.trace("Send transit Ping to {}", target.address());
    transport.send(target.address(), pingMessage);
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and sends it to
   * ORIGINAL_ISSUER.
   */
  private void onTransitPingAck(Message message) {
    LOGGER.trace("Received transit PingAck: {}", message);
    PingData data = message.data();
    Member target = data.getOriginalIssuer();
    String correlationId = message.correlationId();
    PingData originalAckData = new PingData(target, data.getTo());
    Message originalAckMessage = Message.withData(originalAckData)
        .qualifier(PING_ACK)
        .correlationId(correlationId)
        .build();
    LOGGER.trace("Resend transit PingAck to {}", target.address());
    transport.send(target.address(), originalAckMessage);
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

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

  private void publishPingResult(Member member, MemberStatus status) {
    LOGGER.debug("Member {} detected as {}", member, status);
    subject.onNext(new FailureDetectorEvent(member, status));
  }

  private boolean isPing(Message message) {
    return PING.equals(message.qualifier());
  }

  private boolean isPingReq(Message message) {
    return PING_REQ.equals(message.qualifier());
  }

  private boolean isPingAck(Message message) {
    return PING_ACK.equals(message.qualifier()) && message.<PingData>data().getOriginalIssuer() == null;
  }

  private boolean isTransitPingAck(Message message) {
    return PING_ACK.equals(message.qualifier()) && message.<PingData>data().getOriginalIssuer() != null;
  }
}
