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
  private int factor = 1;
  private long gossipCounter = 0;
  private Map<String, GossipState> gossips = Maps.newHashMap();
  private List<Member> remoteMembers = new ArrayList<>();

  // Subscriptions

  private Subscriber<MembershipEvent> onMembershipEventSubscriber;
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
    onMembershipEventSubscriber = Subscribers.create(this::onMembershipEvent);
    membership.listen().observeOn(scheduler)
        .subscribe(onMembershipEventSubscriber);

    onGossipRequestSubscriber = Subscribers.create(this::onGossipReq);
    transport.listen().observeOn(scheduler)
        .filter(this::isGossipReq)
        .subscribe(onGossipRequestSubscriber);

    spreadGossipTask = executor.scheduleWithFixedDelay(this::doSpreadGossip,
        config.getGossipInterval(), config.getGossipInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    // Stop accepting gossip requests
    if (onMembershipEventSubscriber != null) {
      onMembershipEventSubscriber.unsubscribe();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }

    // Stop spreading gossips
    if (spreadGossipTask != null) {
      spreadGossipTask.cancel(true);
    }

    // Shutdown executor
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

  /* ================================================ *
   * ============== Action Methods ================== *
   * ================================================ */

  private void doSpreadGossip() {
    try {
      // Increment period
      period++;

      // Check any gossips to spread
      if (gossips.isEmpty()) {
        return;
      }

      // Spread gossips to random member(s)
      List<Member> gossipMembers = selectGossipMembers();
      for (Member member : gossipMembers) {
        // Select gossips to send
        List<Gossip> gossipsToSend = selectGossipsToSend(member);
        if (gossipsToSend.isEmpty()) {
          continue; // nothing to spread
        }

        // Send gossips
        sendGossips(member, gossipsToSend);

        // Update gossips states
        gossipsToSend.forEach(gossip -> {
            GossipState gossipState = gossips.get(gossip.gossipId());
            gossipState.incrementSpreadCount();
            gossipState.addToInfected(member);
          });
      }

      // Sweep gossips
      sweepGossips();
    } catch (Exception cause) {
      LOGGER.error("Unhandled exception: {}", cause, cause);
    }
  }

  /* ================================================ *
   * ============== Event Listeners ================= *
   * ================================================ */

  private void onMembershipEvent(MembershipEvent event) {
    if (event.isAdded()) {
      remoteMembers.add(event.member());
    } else if (event.isRemoved()) {
      remoteMembers.remove(event.member());
    }
    this.factor = 32 - Integer.numberOfLeadingZeros(remoteMembers.size() + 1);
  }

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
      gossipState.addToInfected(gossipRequest.from());
    }
  }

  /* ================================================ *
   * ============== Helper Methods ================== *
   * ================================================ */

  private boolean isGossipReq(Message message) {
    return GOSSIP_REQ.equals(message.qualifier());
  }

  private String generateGossipId() {
    return membership.member().id() + "-" + gossipCounter++;
  }

  private List<Gossip> selectGossipsToSend(Member member) {
    return gossips.values().stream()
        .filter(gossipState -> !gossipState.isInfected(member))
        .filter(gossipState -> gossipState.spreadCount() < config.getMaxGossipSent() * factor)
        .map(GossipState::gossip)
        .collect(Collectors.toList());
  }

  private List<Member> selectGossipMembers() {
    if (remoteMembers.size() < config.getMaxMembersToSelect()) {
      return remoteMembers; // all
    } else if (config.getMaxMembersToSelect() == 1) {
      return Collections.singletonList(remoteMembers.get(ThreadLocalRandom.current().nextInt(remoteMembers.size())));
    } else {
      Collections.shuffle(remoteMembers);
      return remoteMembers.subList(0, config.getMaxMembersToSelect());
    }
  }

  private void sendGossips(Member to, List<Gossip> gossips) {
    GossipRequest gossipReqData = new GossipRequest(gossips, membership.member());
    Message gossipReqMsg = Message.withData(gossipReqData).qualifier(GOSSIP_REQ).build();
    transport.send(to.address(), gossipReqMsg);
  }

  private void sweepGossips() {
    int maxPeriods = factor * 10;
    Set<GossipState> gossipsToRemove = gossips.values().stream()
        .filter(gossipState -> period > gossipState.infectionPeriod() + maxPeriods)
        .collect(Collectors.toSet());
    if (!gossipsToRemove.isEmpty()) {
      LOGGER.debug("Sweep gossips: {}", gossipsToRemove);
      for (GossipState gossipState : gossipsToRemove) {
        gossips.remove(gossipState.gossip().gossipId());
      }
    }
  }

}
