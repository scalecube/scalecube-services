package io.scalecube.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.IMembershipProtocol;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import com.google.common.collect.Maps;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class GossipProtocol implements IGossipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

  // Qualifiers

  public static final String GOSSIP_REQ = "sc/gossip/req";

  // Injected

  private final ITransport transport;
  private final IMembershipProtocol membership;
  private final GossipConfig config;

  // Local State

  private long period = 0;
  private long gossipCounter = 0;
  private Map<String, GossipState> gossips = Maps.newHashMap();
  private List<Member> remoteMembers = new ArrayList<>();
  private int remoteMembersIndex = -1;

  // Subscriptions

  private Subscriber<Member> onMemberAddedEventSubscriber;
  private Subscriber<Member> onMemberRemovedEventSubscriber;
  private Subscriber<Message> onGossipRequestSubscriber;

  // Subject

  private Subject<Message, Message> subject = PublishSubject.<Message>create().toSerialized();

  // Scheduled

  private final ScheduledExecutorService executor;
  private final Scheduler scheduler;
  private ScheduledFuture<?> spreadGossipTask;

  /**
   * Creates new instance of gossip protocol with given memberId, transport and settings.
   *
   * @param transport transport
   * @param membership membership protocol
   * @param config gossip protocol settings
   */
  public GossipProtocol(ITransport transport, IMembershipProtocol membership, GossipConfig config) {
    checkArgument(transport != null);
    checkArgument(membership != null);
    checkArgument(config != null);
    this.transport = transport;
    this.membership = membership;
    this.config = config;
    String nameFormat = "sc-gossip-" + transport.address().toString();
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

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  Member getMember() {
    return membership.member();
  }

  @Override
  public void start() {
    onMemberAddedEventSubscriber = Subscribers.create(remoteMembers::add, this::onError);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isAdded)
        .map(MembershipEvent::member)
        .subscribe(onMemberAddedEventSubscriber);

    onMemberRemovedEventSubscriber = Subscribers.create(remoteMembers::remove, this::onError);
    membership.listen().observeOn(scheduler)
        .filter(MembershipEvent::isRemoved)
        .map(MembershipEvent::member)
        .subscribe(onMemberRemovedEventSubscriber);

    onGossipRequestSubscriber = Subscribers.create(this::onGossipReq, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(this::isGossipReq)
        .subscribe(onGossipRequestSubscriber);

    spreadGossipTask = executor.scheduleWithFixedDelay(this::doSpreadGossip,
        config.getGossipInterval(), config.getGossipInterval(), TimeUnit.MILLISECONDS);
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  @Override
  public void stop() {
    // Stop accepting gossip requests
    if (onMemberAddedEventSubscriber != null) {
      onMemberAddedEventSubscriber.unsubscribe();
    }
    if (onMemberRemovedEventSubscriber != null) {
      onMemberRemovedEventSubscriber.unsubscribe();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }

    // Stop spreading gossips
    if (spreadGossipTask != null) {
      spreadGossipTask.cancel(true);
    }

    // Shutdown executor
    // TODO AK: Consider to await termination ?!
    executor.shutdown();

    // Stop publishing events
    subject.onCompleted();
  }

  @Override
  public void spread(Message message) {
    executor.execute(() -> onSpreadGossip(message));
  }

  @Override
  public Observable<Message> listen() {
    return subject.asObservable();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doSpreadGossip() {
    // Increment period
    period++;

    // Check any gossips exists
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    try {
      // Select gossips to send
      List<Gossip> gossipsToSend = selectGossipsToSend();
      if (gossipsToSend.isEmpty()) {
        return; // nothing to spread
      }

      // Spread gossips to randomly selected member(s)
      Message gossipReqMsg = buildGossipRequestMessage(gossipsToSend);
      selectGossipMembers(config.getGossipFanout())
          .forEach(member -> transport.send(member.address(), gossipReqMsg));

      // Sweep gossips
      sweepGossips();
    } catch (Exception cause) {
      LOGGER.error("Exception on sending GossipReq[{}] exception: {}", period, cause.getMessage(), cause);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private void onSpreadGossip(Message message) {
    Gossip gossip = new Gossip(generateGossipId(), message);
    GossipState gossipState = new GossipState(gossip, period);
    gossips.put(gossip.gossipId(), gossipState);
  }

  private void onGossipReq(Message message) {
    GossipRequest gossipRequest = message.data();
    for (Gossip gossip : gossipRequest.gossips()) {
      GossipState gossipState = gossips.get(gossip.gossipId());
      if (gossipState == null) { // new gossip
        gossipState = new GossipState(gossip, period);
        gossips.put(gossip.gossipId(), gossipState);
        subject.onNext(gossip.message());
      }
    }
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private boolean isGossipReq(Message message) {
    return GOSSIP_REQ.equals(message.qualifier());
  }

  private String generateGossipId() {
    return membership.member().id() + "-" + gossipCounter++;
  }

  private List<Gossip> selectGossipsToSend() {
    int maxPeriodsToSpread = maxPeriodsToSpread();
    return gossips.values().stream()
        .filter(gossipState -> gossipState.infectionPeriod() + maxPeriodsToSpread >= period) // max rounds
        .map(GossipState::gossip)
        .collect(Collectors.toList());
  }

  private List<Member> selectGossipMembers(int gossipFanout) {
    if (remoteMembers.size() < gossipFanout) { // select all
      return remoteMembers;
    } else { // select random members
      // Shuffle members initially and once reached top bound
      if (remoteMembersIndex < 0 || remoteMembersIndex + gossipFanout > remoteMembers.size()) {
        Collections.shuffle(remoteMembers);
        remoteMembersIndex = 0;
      }

      // Select members
      List<Member> selectedMembers = gossipFanout == 1
          ? Collections.singletonList(remoteMembers.get(remoteMembersIndex))
          : remoteMembers.subList(remoteMembersIndex, remoteMembersIndex + gossipFanout);

      // Increment index and return result
      remoteMembersIndex += gossipFanout;
      return selectedMembers;
    }
  }

  private Message buildGossipRequestMessage(List<Gossip> gossipsToSend) {
    GossipRequest gossipReqData = new GossipRequest(gossipsToSend, membership.member());
    return Message.withData(gossipReqData).qualifier(GOSSIP_REQ).build();
  }

  private void sweepGossips() {
    // Select gossips to sweep
    int maxPeriodsToKeep = maxPeriodsToKeep();
    Set<GossipState> gossipsToRemove = gossips.values().stream()
        .filter(gossipState -> period > gossipState.infectionPeriod() + maxPeriodsToKeep)
        .collect(Collectors.toSet());

    // Check if anything selected
    if (gossipsToRemove.isEmpty()) {
      return; // nothing to sweep
    }

    // Sweep gossips
    LOGGER.debug("Sweep gossips: {}", gossipsToRemove);
    for (GossipState gossipState : gossipsToRemove) {
      gossips.remove(gossipState.gossip().gossipId());
    }
  }

  private int maxPeriodsToKeep() {
    return 2 * maxPeriodsToSpread() + 1;
  }

  private int maxPeriodsToSpread() {
    return factor() + 1;
  }

  private int factor() {
    return 32 - Integer.numberOfLeadingZeros(remoteMembers.size() + 1) /* ceil[ log2( N + 1 ) ] */;
  }

}
