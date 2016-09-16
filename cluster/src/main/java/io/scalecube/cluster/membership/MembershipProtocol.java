package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.ALIVE;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.MembershipEvent;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.IFailureDetector;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class MembershipProtocol implements IMembershipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

  // qualifiers
  public static final String SYNC = "io.scalecube.cluster/membership/sync";
  public static final String SYNC_ACK = "io.scalecube.cluster/membership/syncAck";
  public static final String MEMBERSHIP_GOSSIP = "io.scalecube.cluster/membership/gossip";

  // Injected

  private final Member localMember;
  private final ITransport transport;
  private final MembershipConfig config;
  private final IFailureDetector failureDetector;
  private final IGossipProtocol gossipProtocol;
  private final List<Address> seedMembers;

  // State

  private final ConcurrentMap<String, MembershipRecord> membershipTable = new ConcurrentHashMap<>();

  // Subscriptions

  private final Subject<MembershipEvent, MembershipEvent> subject =
      PublishSubject.<MembershipEvent>create().toSerialized();

  private Subscriber<Message> onSyncRequestSubscriber;
  private Subscriber<Message> onSyncAckResponseSubscriber;
  private Subscriber<FailureDetectorEvent> onFdEventSubscriber;
  private Subscriber<Message> onGossipRequestSubscriber;

  // Scheduled

  private final Scheduler scheduler;
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> syncTask;
  private final Map<String, ScheduledFuture<?>> removeMemberTasks = new HashMap<>();

  /**
   * Creates new instantiates of cluster membership protocol with given transport and config.
   *
   * @param memberId id of this member
   * @param transport transport
   * @param config membership config parameters
   */
  public MembershipProtocol(String memberId, ITransport transport, MembershipConfig config,
      IFailureDetector failureDetector, IGossipProtocol gossipProtocol) {
    this.transport = transport;
    this.config = config;
    this.gossipProtocol = gossipProtocol;
    this.failureDetector = failureDetector;
    this.localMember = new Member(memberId, transport.address(), config.getMetadata());
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

  IFailureDetector getFailureDetector() {
    return failureDetector;
  }

  IGossipProtocol getGossipProtocol() {
    return gossipProtocol;
  }

  ITransport getTransport() {
    return transport;
  }

  List<MembershipRecord> getMembershipRecords() {
    return new ArrayList<>(membershipTable.values());
  }

  @Override
  public Observable<MembershipEvent> listen() {
    return subject.asObservable();
  }

  @Override
  public List<Member> members() {
    List<MembershipRecord> membershipRecords = new ArrayList<>(membershipTable.values());
    return membershipRecords.stream().map(MembershipRecord::member).collect(Collectors.toList());
  }

  @Override
  public List<Member> otherMembers() {
    List<Member> members = members();
    members.remove(localMember);
    return members;
  }

  @Override
  public Member member(String id) {
    checkArgument(!Strings.isNullOrEmpty(id), "Member id can't be null or empty");
    MembershipRecord membershipRecord = membershipTable.get(id);
    return membershipRecord != null ? membershipRecord.member() : null;
  }

  @Override
  public Member member(Address address) {
    checkArgument(address != null, "Member address can't be null or empty");
    return findMemberByAddress(address).member();
  }

  @Override
  public Member member() {
    return localMember;
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public ListenableFuture<Void> start() {
    // Register itself initially before SYNC/SYNC_ACK
    MembershipRecord joinRecord = new MembershipRecord(localMember, ALIVE, 0);
    List<MembershipRecord> updates = merge(joinRecord);
    processUpdates(updates, false/* spread gossip */);

    // Listen to incoming SYNC requests from other members
    onSyncRequestSubscriber = Subscribers.create(this::onSync);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC.equals(msg.qualifier()))
        .filter(this::checkSyncGroup)
        .subscribe(onSyncRequestSubscriber);

    // Listen to incomming SYNC ACK responses from other members
    onSyncAckResponseSubscriber = Subscribers.create(this::onSyncAck);
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
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

    // Schedule sending periodic sync to random sync address
    syncTask = executor.scheduleWithFixedDelay(
        this::doSync, config.getSyncInterval(), config.getSyncInterval(), TimeUnit.MILLISECONDS);

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

  private ListenableFuture<Void> doInitialSync() {
    LOGGER.debug("Making initial Sync to all seed members: {}", seedMembers);
    if (seedMembers.isEmpty()) {
      return Futures.immediateFuture(null);
    }

    SettableFuture<Void> syncResponseFuture = SettableFuture.create();

    // Listen initial Sync Ack
    transport.listen().observeOn(scheduler)
        .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
        .filter(this::checkSyncGroup)
        .take(1)
        .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS, scheduler)
        .subscribe(message -> {
            onSyncAck(message);
            syncResponseFuture.set(null);
          }, throwable -> {
            LOGGER.info("Timeout getting initial SyncAck from seed members: {}", seedMembers);
            syncResponseFuture.set(null);
          });

    Message syncMsg = prepareSyncMessage(SYNC);
    for (Address address : seedMembers) {
      transport.send(address, syncMsg);
    }

    return syncResponseFuture;
  }

  private void doSync() {
    try {
      Address syncMember = selectSyncAddress();
      if (syncMember == null) {
        return;
      }
      LOGGER.debug("Sending Sync to: {}", syncMember);
      transport.send(syncMember, prepareSyncMessage(SYNC));
    } catch (Exception cause) {
      LOGGER.error("Unhandled exception: {}", cause, cause);
    }
  }

  private Address selectSyncAddress() {
    // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
    return !seedMembers.isEmpty() ? seedMembers.get(ThreadLocalRandom.current().nextInt(seedMembers.size())) : null;
  }

  private boolean checkSyncGroup(Message message) {
    MembershipData data = message.data();
    return config.getSyncGroup().equals(data.getSyncGroup());
  }

  private Message prepareSyncMessage(String qualifier) {
    List<MembershipRecord> membershipRecords = new ArrayList<>(membershipTable.values());
    MembershipData syncData = new MembershipData(membershipRecords, config.getSyncGroup());
    return Message.withData(syncData).qualifier(qualifier).build();
  }

  private void onSyncAck(Message message) {
    MembershipData data = message.data();
    List<MembershipRecord> updates = merge(data);
    LOGGER.debug("Received SyncAck from {}, updates: {}", message.sender(), updates);
    if (!updates.isEmpty()) {
      processUpdates(updates, true/* spread gossip */);
    }
  }

  /**
   * Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK.
   */
  private void onSync(Message message) {
    MembershipData data = message.data();
    List<MembershipRecord> updates = merge(data);
    Address sender = message.sender();
    if (!updates.isEmpty()) {
      LOGGER.debug("Received Sync from {}, updates: {}", sender, updates);
      processUpdates(updates, true/* spread gossip */);
    } else {
      LOGGER.debug("Received Sync from {}, no updates", sender);
    }
    transport.send(sender, prepareSyncMessage(SYNC_ACK));
  }

  /**
   * Merges FD updates and processes them.
   */
  private void onFailureDetectorEvent(FailureDetectorEvent fdEvent) {
    MembershipRecord r0 = findMemberByAddress(fdEvent.address());
    if (r0 != null) {
      List<MembershipRecord> updates = merge(new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation()));
      if (!updates.isEmpty()) {
        LOGGER.debug("Received FD event {}, updates: {}", fdEvent, updates);
        processUpdates(updates, true/* spread gossip */);
      }
    }
  }

  /**
   * Merges gossip's {@link MembershipData} (not spreading gossip further).
   */
  private void onMembershipGossip(Message message) {
    MembershipData data = message.data();
    List<MembershipRecord> updates = merge(data);
    if (!updates.isEmpty()) {
      LOGGER.debug("Received gossip, updates: {}", updates);
      processUpdates(updates, false/* spread gossip */);
    }
  }

  /**
   * Takes {@code updates} and process them in next order.
   * <ul>
   * <li>recalculates 'cluster members' for {@link #gossipProtocol} and {@link #failureDetector} by filtering out
   * {@code REMOVED/SHUTDOWN} members</li>
   * <li>if {@code spreadGossip} was set {@code true} -- converts {@code updates} to {@link MembershipData} and send it
   * to cluster via {@link #gossipProtocol}</li>
   * <li>publishes updates locally (see {@link #listen()})</li>
   * <li>iterates on {@code updates}, if {@code update} become {@code SUSPECTED} -- schedules a timer for
   * {@code maxSuspectTime} to remove the member (on {@code TRUSTED} -- cancels the timer)</li>
   * <li>iterates on {@code updates}, if {@code update} become {@code SHUTDOWN} -- schedules a timer for
   * {@code maxShutdownTime} to remove the member</li>
   * </ul>
   *
   * @param updates list of updates after merge
   * @param spreadGossip flag indicating should updates be gossiped to cluster
   */
  private void processUpdates(List<MembershipRecord> updates, boolean spreadGossip) {
    if (updates.isEmpty()) {
      return;
    }

    // Reset cluster members on FailureDetector and Gossip
    Collection<Address> members = membershipTable.values().stream()
        .map(MembershipRecord::address)
        .collect(Collectors.toList());
    failureDetector.setMembers(members);
    gossipProtocol.setMembers(members);

    // Publish updates to cluster
    if (spreadGossip) {
      MembershipData membershipData = new MembershipData(updates, config.getSyncGroup());
      Message membershipMsg = Message.withData(membershipData).qualifier(MEMBERSHIP_GOSSIP).build();
      gossipProtocol.spread(membershipMsg);
    }

    // Publish updates locally
    for (MembershipRecord update : updates) {
      // TODO: Publish only relevant membership events
      //subject.onNext(update);
    }

    // Check state transition
    for (final MembershipRecord record : updates) {
      LOGGER.debug("Membership update: {}", record);
      switch (record.status()) {
        case SUSPECT:
          // schedule suspected member remove
          removeMemberTasks.putIfAbsent(record.id(), executor.schedule(() -> {
              LOGGER.debug("Time to remove SUSPECTED member={} from membership table", record);
              removeMemberTasks.remove(record.id());
              MembershipRecord r0 = membershipTable.remove(record.id());
              if (r0 != null) {
                processUpdates(
                    Collections.singletonList(new MembershipRecord(r0.member(), DEAD, r0.incarnation())),
                    false/* spread gossip */);
              }
            }, config.getSuspectTimeout(), TimeUnit.MILLISECONDS));
          break;
        case ALIVE:
          // clean schedule
          ScheduledFuture<?> future = removeMemberTasks.remove(record.id());
          if (future != null) {
            future.cancel(true);
          }
          break;
        case DEAD:
          // TODO: process correctly
          break;
      }
    }
  }


  // TODO: Membership table stuff

  public List<MembershipRecord> merge(MembershipData data) {
    List<MembershipRecord> updates = new ArrayList<>();
    for (MembershipRecord record : data.getMembership()) {
      updates.addAll(merge(record));
    }
    return updates;
  }

  public List<MembershipRecord> merge(MembershipRecord r1) {
    List<MembershipRecord> updates = new ArrayList<>(1);
    MembershipRecord r0 = membershipTable.putIfAbsent(r1.id(), r1);
    if (r0 == null) {
      updates.add(r1);
    } else if (r0.compareTo(r1) < 0) {
      if (membershipTable.replace(r1.id(), r0, r1)) {
        updates.add(r1);
      } else {
        return merge(r1);
      }
    }
    return updates;
  }

  public MembershipRecord findMemberByAddress(Address address) {
    // TODO [AK]: Temporary solution, should be optimized!!!
    for (MembershipRecord member : membershipTable.values()) {
      if (member.address().equals(address)) {
        return member;
      }
    }
    return null;
  }

}
