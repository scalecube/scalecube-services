package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class MembershipProtocolImpl implements MembershipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocolImpl.class);

  private enum MembershipUpdateReason {
    FAILURE_DETECTOR_EVENT,
    MEMBERSHIP_GOSSIP,
    SYNC,
    INITIAL_SYNC,
    SUSPICION_TIMEOUT
  }

  // Qualifiers

  public static final String SYNC = "sc/membership/sync";
  public static final String SYNC_ACK = "sc/membership/syncAck";
  public static final String MEMBERSHIP_GOSSIP = "sc/membership/gossip";

  // Injected

  private final AtomicReference<Member> memberRef;
  private final Transport transport;
  private final MembershipConfig config;
  private final List<Address> seedMembers;
  private FailureDetector failureDetector;
  private GossipProtocol gossipProtocol;

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
  private final Map<String, ScheduledFuture<?>> suspicionTimeoutTasks = new HashMap<>();
  private ScheduledFuture<?> syncTask;

  /**
   * Creates new instantiates of cluster membership protocol with given transport and config.
   *
   * @param transport transport
   * @param config membership config parameters
   */
  public MembershipProtocolImpl(Transport transport, MembershipConfig config) {
    this.transport = transport;
    this.config = config;

    Address address = memberAddress(transport, config);
    Member member = new Member(IdGenerator.generateId(), address, config.getMetadata());
    this.memberRef = new AtomicReference<>(member);

    String nameFormat = "sc-membership-" + Integer.toString(address.port());
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());

    this.scheduler = Schedulers.from(executor);
    this.seedMembers = cleanUpSeedMembers(config.getSeedMembers());
  }

  /**
   * Returns the accessible member address, either from the transport or the overridden variables.
   * @param transport transport
   * @param config membership config parameters
   * @return Accessible member address
   */
  protected static Address memberAddress(Transport transport, MembershipConfig config) {
    Address memberAddress = transport.address();
    if (config.getMemberHost() != null) {
      int memberPort = config.getMemberPort() != null ? config.getMemberPort() : memberAddress.port();
      memberAddress = Address.create(config.getMemberHost(), memberPort);
    }

    return memberAddress;
  }

  // Remove duplicates and local address
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers) {
    Set<Address> seedMembersSet = new HashSet<>(seedMembers); // remove duplicates
    seedMembersSet.remove(member().address()); // remove local address
    return Collections.unmodifiableList(new ArrayList<>(seedMembersSet));
  }

  public void setFailureDetector(FailureDetector failureDetector) {
    this.failureDetector = failureDetector;
  }

  public void setGossipProtocol(GossipProtocol gossipProtocol) {
    this.gossipProtocol = gossipProtocol;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  FailureDetector getFailureDetector() {
    return failureDetector;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  GossipProtocol getGossipProtocol() {
    return gossipProtocol;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  Transport getTransport() {
    return transport;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  List<MembershipRecord> getMembershipRecords() {
    return ImmutableList.copyOf(membershipTable.values());
  }

  @Override
  public Observable<MembershipEvent> listen() {
    return subject.onBackpressureBuffer().asObservable();
  }

  @Override
  public Member member() {
    return memberRef.get();
  }

  @Override
  public void updateMetadata(Map<String, String> metadata) {
    executor.execute(() -> onUpdateMetadata(metadata));
  }

  @Override
  public void updateMetadataProperty(String key, String value) {
    executor.execute(() -> onUpdateMetadataProperty(key, value));
  }

  /**
   * Spreads leave notification to other cluster members.
   */
  public CompletableFuture<String> leave() {
    CompletableFuture<String> future = new CompletableFuture<>();
    executor.execute(() -> {
      CompletableFuture<String> leaveFuture = onLeave();
      leaveFuture.whenComplete((gossipId, error) -> {
        future.complete(gossipId);
      });
    });
    return future;
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public CompletableFuture<Void> start() {
    // Init membership table with local member record
    Member member = memberRef.get();
    MembershipRecord localMemberRecord = new MembershipRecord(member, ALIVE, 0);
    membershipTable.put(member.id(), localMemberRecord);

    // Listen to incoming SYNC requests from other members
    onSyncRequestSubscriber = Subscribers.create(this::onSync, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC.equals(msg.qualifier()))
        .filter(this::checkSyncGroup)
        .subscribe(onSyncRequestSubscriber);

    // Listen to incoming SYNC ACK responses from other members
    onSyncAckResponseSubscriber = Subscribers.create(this::onSyncAck, this::onError);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
        .filter(msg -> msg.correlationId() == null) // filter out initial sync
        .filter(this::checkSyncGroup)
        .subscribe(onSyncAckResponseSubscriber);

    // Listen to events from failure detector
    onFdEventSubscriber = Subscribers.create(this::onFailureDetectorEvent, this::onError);
    failureDetector.listen().observeOn(scheduler)
        .subscribe(onFdEventSubscriber);

    // Listen to membership gossips
    onGossipRequestSubscriber = Subscribers.create(this::onMembershipGossip, this::onError);
    gossipProtocol.listen().observeOn(scheduler)
        .filter(msg -> MEMBERSHIP_GOSSIP.equals(msg.qualifier()))
        .subscribe(onGossipRequestSubscriber);

    // Make initial sync with all seed members
    return doInitialSync();
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
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
    for (String memberId : suspicionTimeoutTasks.keySet()) {
      ScheduledFuture<?> future = suspicionTimeoutTasks.get(memberId);
      if (future != null) {
        future.cancel(true);
      }
    }
    suspicionTimeoutTasks.clear();

    // Shutdown executor
    executor.shutdown();

    // Stop publishing events
    subject.onCompleted();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private CompletableFuture<Void> doInitialSync() {
    LOGGER.debug("Making initial Sync to all seed members: {}", seedMembers);
    if (seedMembers.isEmpty()) {
      schedulePeriodicSync();
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> syncResponseFuture = new CompletableFuture<>();

    // Listen initial Sync Ack
    String cid = memberRef.get().id();
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
        .filter(msg -> cid.equals(msg.correlationId()))
        .filter(this::checkSyncGroup)
        .take(1)
        .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS, scheduler)
        .subscribe(
            message -> {
              SyncData syncData = message.data();
              LOGGER.info("Joined cluster '{}': {}", syncData.getSyncGroup(), syncData.getMembership());
              onSyncAck(message, true);
              schedulePeriodicSync();
              syncResponseFuture.complete(null);
            },
            throwable -> {
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

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private void onUpdateMetadataProperty(String key, String value) {
    // Update local member reference
    Member curMember = memberRef.get();
    Map<String, String> metadata = new HashMap<>(curMember.metadata());
    metadata.put(key, value);
    onUpdateMetadata(metadata);
  }

  private void onUpdateMetadata(Map<String, String> metadata) {
    // Update local member reference
    Member curMember = memberRef.get();
    String memberId = curMember.id();
    Member newMember = new Member(memberId, curMember.address(), metadata);
    memberRef.set(newMember);

    // Update membership table
    MembershipRecord curRecord = membershipTable.get(memberId);
    MembershipRecord newRecord = new MembershipRecord(newMember, ALIVE, curRecord.incarnation() + 1);
    membershipTable.put(memberId, newRecord);

    // Emit membership updated event
    subject.onNext(MembershipEvent.createUpdated(curMember, newMember));

    // Spread new membership record over the cluster
    spreadMembershipGossip(newRecord);
  }

  private void onSyncAck(Message syncAckMsg) {
    onSyncAck(syncAckMsg, false);
  }

  private void onSyncAck(Message syncAckMsg, boolean initial) {
    LOGGER.debug("Received SyncAck: {}", syncAckMsg);
    syncMembership(syncAckMsg.data(), initial);
  }

  /**
   * Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK.
   */
  private void onSync(Message syncMsg) {
    LOGGER.debug("Received Sync: {}", syncMsg);
    syncMembership(syncMsg.data(), false);
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
    if (fdEvent.status() == ALIVE) {
      // TODO: Consider to make more elegant solution
      // Alive won't override SUSPECT so issue instead extra sync with member to force it spread alive with inc + 1
      Message syncMsg = prepareSyncDataMsg(SYNC, null);
      transport.send(fdEvent.member().address(), syncMsg);
    } else {
      MembershipRecord r1 = new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation());
      updateMembership(r1, MembershipUpdateReason.FAILURE_DETECTOR_EVENT);
    }
  }

  /**
   * Merges received membership gossip (not spreading gossip further).
   */
  private void onMembershipGossip(Message message) {
    MembershipRecord record = message.data();
    LOGGER.debug("Received membership gossip: {}", record);
    updateMembership(record, MembershipUpdateReason.MEMBERSHIP_GOSSIP);
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

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

  private void syncMembership(SyncData syncData, boolean initial) {
    for (MembershipRecord r1 : syncData.getMembership()) {
      MembershipRecord r0 = membershipTable.get(r1.id());
      if (!r1.equals(r0)) {
        MembershipUpdateReason reason = initial ? MembershipUpdateReason.INITIAL_SYNC : MembershipUpdateReason.SYNC;
        updateMembership(r1, reason);
      }
    }
  }

  /**
   * Try to update membership table with the given record.
   *
   * @param r1 new membership record which compares with existing r0 record
   * @param reason indicating the reason for updating membership table
   */
  private void updateMembership(MembershipRecord r1, MembershipUpdateReason reason) {
    Preconditions.checkArgument(r1 != null, "Membership record can't be null");

    // Get current record
    MembershipRecord r0 = membershipTable.get(r1.id());

    // Check if new record r1 overrides existing membership record r0
    if (!r1.isOverrides(r0)) {
      return;
    }

    // If received updated for local member then increase incarnation number and spread Alive gossip
    Member localMember = memberRef.get();
    if (r1.member().id().equals(localMember.id())) {
      int currentIncarnation = Math.max(r0.incarnation(), r1.incarnation());
      MembershipRecord r2 = new MembershipRecord(localMember, r0.status(), currentIncarnation + 1);
      membershipTable.put(localMember.id(), r2);
      LOGGER.debug("Local membership record r0={}, but received r1={}, spread r2={}", r0, r1, r2);
      spreadMembershipGossip(r2);
      return;
    }

    // Update membership
    if (r1.isDead()) {
      membershipTable.remove(r1.id());
    } else {
      membershipTable.put(r1.id(), r1);
    }

    // Schedule/cancel suspicion timeout task
    if (r1.isSuspect()) {
      scheduleSuspicionTimeoutTask(r1);
    } else {
      cancelSuspicionTimeoutTask(r1.id());
    }

    // Emit membership event
    if (r1.isDead()) {
      subject.onNext(MembershipEvent.createRemoved(r1.member()));
    } else if (r0 == null && r1.isAlive()) {
      subject.onNext(MembershipEvent.createAdded(r1.member()));
    } else if (r0 != null && !r0.member().equals(r1.member())) {
      subject.onNext(MembershipEvent.createUpdated(r0.member(), r1.member()));
    }


    // Spread gossip (unless already gossiped)
    if (reason != MembershipUpdateReason.MEMBERSHIP_GOSSIP && reason != MembershipUpdateReason.INITIAL_SYNC) {
      spreadMembershipGossip(r1);
    }
  }

  private void cancelSuspicionTimeoutTask(String memberId) {
    ScheduledFuture<?> future = suspicionTimeoutTasks.remove(memberId);
    if (future != null) {
      future.cancel(true);
    }
  }

  private void scheduleSuspicionTimeoutTask(MembershipRecord record) {
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(config.getSuspicionMult(), membershipTable.size(), config.getPingInterval());
    suspicionTimeoutTasks.computeIfAbsent(record.id(),
        id -> executor.schedule(() -> onSuspicionTimeout(id), suspicionTimeout, TimeUnit.MILLISECONDS));
  }

  private void onSuspicionTimeout(String memberId) {
    suspicionTimeoutTasks.remove(memberId);
    MembershipRecord record = membershipTable.get(memberId);
    if (record != null) {
      LOGGER.debug("Declare SUSPECTED member as DEAD by timeout: {}", record);
      MembershipRecord deadRecord = new MembershipRecord(record.member(), DEAD, record.incarnation());
      updateMembership(deadRecord, MembershipUpdateReason.SUSPICION_TIMEOUT);
    }
  }

  private CompletableFuture<String> onLeave() {
    Member curMember = memberRef.get();
    String memberId = curMember.id();
    MembershipRecord curRecord = membershipTable.get(memberId);
    MembershipRecord newRecord = new MembershipRecord(this.member(), DEAD, curRecord.incarnation() + 1);
    membershipTable.put(memberId, newRecord);
    return spreadMembershipGossip(newRecord);
  }

  private CompletableFuture<String> spreadMembershipGossip(MembershipRecord record) {
    Message membershipMsg = Message.withData(record).qualifier(MEMBERSHIP_GOSSIP).build();
    return gossipProtocol.spread(membershipMsg);
  }

}
