package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.ALIVE;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.IFailureDetector;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import com.google.common.base.Preconditions;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

public final class MembershipProtocol implements IMembershipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

  // Qualifiers

  public static final String SYNC = "sc/membership/sync";
  public static final String SYNC_ACK = "sc/membership/syncAck";
  public static final String MEMBERSHIP_GOSSIP = "sc/membership/gossip";

  // Injected

  private final Member member;
  private final ITransport transport;
  private final MembershipConfig config;
  private final List<Address> seedMembers;
  private IFailureDetector failureDetector;
  private IGossipProtocol gossipProtocol;

  // State

  private final Map<String, MembershipRecord> membershipTable = new HashMap<>();

  // Subject

  private final Subject<MembershipEvent, MembershipEvent> subject =
      PublishSubject.<MembershipEvent>create().toSerialized();

  // Subscriptions

  private Subscriber<Message> onSyncRequestSubscriber;
  private Subscriber<Message> onSyncAckResponseSubscriber;
  private Subscriber<FailureDetectorEvent> onFdEventSubscriber;
  private Subscriber<Message> onGossipRequestSubscriber;

  // Scheduled

  private final Scheduler scheduler;
  private final ScheduledExecutorService executor;
  private final Map<String, ScheduledFuture<?>> removeMemberTasks = new HashMap<>();
  private ScheduledFuture<?> syncTask;

  /**
   * Creates new instantiates of cluster membership protocol with given transport and config.
   *
   * @param transport transport
   * @param config membership config parameters
   */
  public MembershipProtocol(ITransport transport, MembershipConfig config) {
    this.transport = transport;
    this.config = config;
    this.member = new Member(IdGenerator.generateId(), transport.address(), config.getMetadata());

    String nameFormat = "sc-membership-" + transport.address().toString();
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());

    this.scheduler = Schedulers.from(executor);
    this.seedMembers = cleanUpSeedMembers(config.getSeedMembers());
  }

  // Remove duplicates and local address
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers) {
    Set<Address> seedMembersSet = new HashSet<>(seedMembers); // remove duplicates
    seedMembersSet.remove(transport.address()); // remove local address
    return Collections.unmodifiableList(new ArrayList<>(seedMembersSet));
  }

  public void setFailureDetector(IFailureDetector failureDetector) {
    this.failureDetector = failureDetector;
  }

  public void setGossipProtocol(IGossipProtocol gossipProtocol) {
    this.gossipProtocol = gossipProtocol;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  IFailureDetector getFailureDetector() {
    return failureDetector;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  IGossipProtocol getGossipProtocol() {
    return gossipProtocol;
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
  List<MembershipRecord> getMembershipRecords() {
    return new ArrayList<>(membershipTable.values());
  }

  @Override
  public Observable<MembershipEvent> listen() {
    return subject.asObservable();
  }

  @Override
  public Member member() {
    return member;
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public CompletableFuture<Void> start() {
    // Init membership table with local member record
    MembershipRecord localMemberRecord = new MembershipRecord(member, ALIVE, 0);
    membershipTable.put(member.id(), localMemberRecord);

    // Listen to incoming SYNC requests from other members
    onSyncRequestSubscriber = Subscribers.create(this::onSync);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC.equals(msg.qualifier()))
        .filter(this::checkSyncGroup)
        .subscribe(onSyncRequestSubscriber);

    // Listen to incoming SYNC ACK responses from other members
    onSyncAckResponseSubscriber = Subscribers.create(this::onSyncAck);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
        .filter(msg -> msg.correlationId() == null) // filter out initial sync
        .filter(this::checkSyncGroup)
        .subscribe(onSyncAckResponseSubscriber);

    // Listen to events from failure detector
    onFdEventSubscriber = Subscribers.create(this::onFailureDetectorEvent);
    failureDetector.listen().observeOn(scheduler)
        .subscribe(onFdEventSubscriber);

    // Listen to membership gossips
    onGossipRequestSubscriber = Subscribers.create(this::onMembershipGossip);
    gossipProtocol.listen().observeOn(scheduler)
        .filter(msg -> MEMBERSHIP_GOSSIP.equals(msg.qualifier()))
        .subscribe(onGossipRequestSubscriber);

    // Make initial sync with all seed members
    return doInitialSync();
  }

  /**
   * Stops running cluster membership protocol and releases occupied resources.
   */
  public void stop() {
    // Stop accepting requests and events
    if (onSyncRequestSubscriber != null) {
      onSyncRequestSubscriber.unsubscribe();
    }
    if (onFdEventSubscriber != null) {
      onFdEventSubscriber.unsubscribe();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }
    if (onSyncAckResponseSubscriber != null) {
      onSyncAckResponseSubscriber.unsubscribe();
    }

    // Stop sending sync
    if (syncTask != null) {
      syncTask.cancel(true);
    }

    // Cancel remove members tasks
    for (String memberId : removeMemberTasks.keySet()) {
      ScheduledFuture<?> future = removeMemberTasks.remove(memberId);
      if (future != null) {
        future.cancel(true);
      }
    }

    // Shutdown executor
    executor.shutdown();

    // Stop publishing events
    subject.onCompleted();
  }

  /* ================================================ *
   * ============== Action Methods ================== *
   * ================================================ */

  private CompletableFuture<Void> doInitialSync() {
    LOGGER.debug("Making initial Sync to all seed members: {}", seedMembers);
    if (seedMembers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> syncResponseFuture = new CompletableFuture<>();

    // Listen initial Sync Ack
    String cid = member.id();
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
        .filter(msg -> cid.equals(msg.correlationId()))
        .filter(this::checkSyncGroup)
        .take(1)
        .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS, scheduler)
        .subscribe(message -> {
            onSyncAck(message);
            schedulePeriodicSync();
            syncResponseFuture.complete(null);
          }, throwable -> {
            LOGGER.info("Timeout getting initial SyncAck from seed members: {}", seedMembers);
            schedulePeriodicSync();
            syncResponseFuture.complete(null);
          });

    Message syncMsg = prepareSyncDataMsg(SYNC, cid);
    seedMembers.forEach(address -> transport.send(address, syncMsg));

    return syncResponseFuture;
  }

  private void doSync() {
    try {
      Address syncMember = selectSyncAddress();
      if (syncMember == null) {
        return;
      }
      Message syncMsg = prepareSyncDataMsg(SYNC, null);
      transport.send(syncMember, syncMsg);
      LOGGER.debug("Send Sync to {}: {}", syncMember, syncMsg);
    } catch (Exception cause) {
      LOGGER.error("Unhandled exception: {}", cause, cause);
    }
  }

  /* ================================================ *
   * ============== Event Listeners ================= *
   * ================================================ */

  private void onSyncAck(Message syncAckMsg) {
    LOGGER.debug("Received SyncAck: {}", syncAckMsg);
    syncMembership(syncAckMsg.data());
  }

  /**
   * Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK.
   */
  private void onSync(Message syncMsg) {
    LOGGER.debug("Received Sync: {}", syncMsg);
    syncMembership(syncMsg.data());
    Message syncAckMsg = prepareSyncDataMsg(SYNC_ACK, syncMsg.correlationId());
    transport.send(syncMsg.sender(), syncAckMsg);
  }

  /**
   * Merges FD updates and processes them.
   */
  private void onFailureDetectorEvent(FailureDetectorEvent fdEvent) {
    MembershipRecord r0 = membershipTable.get(fdEvent.member().id());
    if (r0 == null) { // member already removed
      return;
    }
    if (r0.status() == fdEvent.status()) { // status not changed
      return;
    }
    LOGGER.debug("Received status change on failure detector event: {}", fdEvent);
    MembershipRecord r1 = new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation());
    updateMembership(r1, true /* spread gossip */, false /* don't check override */);
  }

  /**
   * Merges received membership gossip (not spreading gossip further).
   */
  private void onMembershipGossip(Message message) {
    MembershipRecord record = message.data();
    LOGGER.debug("Received membership gossip: {}", record);
    updateMembership(record, false /* don't spread gossip */, true /* check override */);
  }

  /* ================================================ *
   * ============== Helper Methods ================== *
   * ================================================ */

  private Address selectSyncAddress() {
    // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
    return !seedMembers.isEmpty() ? seedMembers.get(ThreadLocalRandom.current().nextInt(seedMembers.size())) : null;
  }

  private boolean checkSyncGroup(Message message) {
    SyncData data = message.data();
    return config.getSyncGroup().equals(data.getSyncGroup());
  }

  private void schedulePeriodicSync() {
    int syncInterval = config.getSyncInterval();
    syncTask = executor.scheduleWithFixedDelay(this::doSync, syncInterval, syncInterval, TimeUnit.MILLISECONDS);
  }

  private Message prepareSyncDataMsg(String qualifier, String cid) {
    List<MembershipRecord> membershipRecords = new ArrayList<>(membershipTable.values());
    SyncData syncData = new SyncData(membershipRecords, config.getSyncGroup());
    return Message.withData(syncData).qualifier(qualifier).correlationId(cid).build();
  }

  private void syncMembership(SyncData syncData) {
    for (MembershipRecord r1 : syncData.getMembership()) {
      MembershipRecord r0 = membershipTable.get(r1.id());
      if (!r1.equals(r0)) {
        updateMembership(r1, true/* spread gossip */, true /* check override */);
      }
    }
  }

  /**
   * Try to update membership table with the given record.
   *
   * @param r1 new membership record which compares with existing r0 record
   * @param spreadGossip flag indicating should updates be gossiped to cluster
   */
  private void updateMembership(MembershipRecord r1, boolean spreadGossip, boolean checkOverride) {
    Preconditions.checkArgument(r1 != null, "Membership record can't be null");

    // Get current record
    MembershipRecord r0 = membershipTable.get(r1.id());

    // Check if r1 overrides existing membership record record
    if (checkOverride && !r1.isOverrides(r0)) {
      return;
    }

    // If received updated for local member then increase incarnation number and spread Alive gossip
    if (r1.member().equals(member)) {
      int currentIncarnation = Math.max(r0.incarnation(), r1.incarnation());
      MembershipRecord r2 = new MembershipRecord(member, ALIVE, currentIncarnation + 1);
      membershipTable.put(member.id(), r2);
      spreadMembershipGossip(r2);
      return;
    }

    // Update membership
    if (r1.isDead()) {
      membershipTable.remove(r1.id());
    } else {
      membershipTable.put(r1.id(), r1);
    }

    // Update remove member tasks
    if (r1.isSuspect()) {
      scheduleRemoveMemberTask(r1);
    } else {
      cancelRemoveMemberTask(r1.id());
    }

    // Emit membership event
    if (r1.isDead() && r0 != null) {
      MembershipEvent membershipEvent = new MembershipEvent(MembershipEvent.Type.REMOVED, r1.member());
      subject.onNext(membershipEvent);
    } else if (r0 == null && !r1.isDead()) {
      MembershipEvent membershipEvent = new MembershipEvent(MembershipEvent.Type.ADDED, r1.member());
      subject.onNext(membershipEvent);
    }

    // Spread gossip
    if (spreadGossip) {
      spreadMembershipGossip(r1);
    }
  }

  private void cancelRemoveMemberTask(String memberId) {
    ScheduledFuture<?> future = removeMemberTasks.remove(memberId);
    if (future != null) {
      future.cancel(true);
    }
  }

  private void scheduleRemoveMemberTask(MembershipRecord record) {
    removeMemberTasks.putIfAbsent(record.id(), executor.schedule(() -> {
        LOGGER.debug("Time to remove SUSPECTED member={} from membership table", record);
        removeMemberTasks.remove(record.id());
        MembershipRecord deadRecord = new MembershipRecord(record.member(), DEAD, record.incarnation());
        updateMembership(deadRecord, true /* spread gossip */, true /* check override */);
      }, config.getSuspectTimeout(), TimeUnit.MILLISECONDS));
  }

  private void spreadMembershipGossip(MembershipRecord record) {
    Message membershipMsg = Message.withData(record).qualifier(MEMBERSHIP_GOSSIP).build();
    gossipProtocol.spread(membershipMsg);
  }

}
