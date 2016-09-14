package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.SHUTDOWN;
import static io.scalecube.cluster.membership.MemberStatus.TRUSTED;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.IFailureDetector;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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

public final class MembershipProtocol implements IMembershipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

  // qualifiers
  public static final String SYNC = "io.scalecube.cluster/membership/sync";
  public static final String SYNC_ACK = "io.scalecube.cluster/membership/syncAck";

  // filters
  private static final Func1<Message, Boolean> GOSSIP_MEMBERSHIP_FILTER =
      msg -> msg.data() != null && MembershipData.class.equals(msg.data().getClass());

  public static final Func1<Message, Boolean> NOT_GOSSIP_MEMBERSHIP_FILTER =
      msg -> msg.data() == null || !MembershipData.class.equals(msg.data().getClass());

  // Injected

  private final Member localMember;
  private final ITransport transport;
  private final MembershipConfig config;
  private final IFailureDetector failureDetector;
  private final IGossipProtocol gossipProtocol;
  private final List<Address> seedMembers;

  // State

  private final MembershipTable membershipTable = new MembershipTable();

  // Subscriptions

  private final Subject<MembershipRecord, MembershipRecord> subject = PublishSubject.<MembershipRecord>create()
      .toSerialized();

  private Subscriber<Message> onSyncRequestSubscriber;
  private Subscriber<Message> onSyncAckResponseSubscriber;
  private Subscriber<FailureDetectorEvent> onFdEventSubscriber;
  private Subscriber<MembershipData> onGossipRequestSubscriber;

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

  @Override
  public Observable<MembershipRecord> listenUpdates() {
    return subject.asObservable();
  }

  @Override
  public List<MembershipRecord> members() {
    return membershipTable.asList();
  }

  @Override
  public List<MembershipRecord> otherMembers() {
    List<MembershipRecord> members = membershipTable.asList();
    members.remove(localMember());
    return members;
  }

  @Override
  public MembershipRecord member(String id) {
    checkArgument(!Strings.isNullOrEmpty(id), "Member id can't be null or empty");
    return membershipTable.get(id);
  }

  @Override
  public MembershipRecord member(Address address) {
    checkArgument(address != null, "Member address can't be null or empty");
    return membershipTable.get(address);
  }

  @Override
  public MembershipRecord localMember() {
    return membershipTable.get(localMember.id());
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public ListenableFuture<Void> start() {
    // Register itself initially before SYNC/SYNC_ACK
    MembershipRecord joinRecord = new MembershipRecord(localMember, TRUSTED);
    List<MembershipRecord> updates = membershipTable.merge(joinRecord);
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
    failureDetector.listenStatus().observeOn(scheduler)
        .subscribe(onFdEventSubscriber);

    // Listen to membership gossips
    onGossipRequestSubscriber = Subscribers.create(this::onMembershipGossip);
    gossipProtocol.listen().observeOn(scheduler)
        .filter(GOSSIP_MEMBERSHIP_FILTER)
        .map(MembershipDataUtils.gossipFilterData(transport.address()))
        .subscribe(onGossipRequestSubscriber);

    // Schedule sending periodic sync to random sync address
    int syncTime = config.getSyncTime();
    syncTask = executor.scheduleWithFixedDelay(this::doSync, syncTime, syncTime, TimeUnit.MILLISECONDS);

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

    Message syncMsg = prepareSyncMessage();
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
      transport.send(syncMember, prepareSyncMessage());
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

  private Message prepareSyncMessage() {
    MembershipData syncData = new MembershipData(membershipTable.asList(), config.getSyncGroup());
    return Message.withData(syncData).qualifier(SYNC).build();
  }

  private void onSyncAck(Message message) {
    MembershipData data = message.data();
    MembershipData filteredData = MembershipDataUtils.filterData(transport.address(), data);
    Address sender = message.sender();
    List<MembershipRecord> updates = membershipTable.merge(filteredData);
    if (!updates.isEmpty()) {
      LOGGER.debug("Received SyncAck from {}, updates: {}", sender, updates);
      processUpdates(updates, true/* spread gossip */);
    } else {
      LOGGER.debug("Received SyncAck from {}, no updates", sender);
    }
  }

  /**
   * Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK.
   */
  private void onSync(Message message) {
    MembershipData data = message.data();
    MembershipData filteredData = MembershipDataUtils.filterData(transport.address(), data);
    List<MembershipRecord> updates = membershipTable.merge(filteredData);
    Address sender = message.sender();
    if (!updates.isEmpty()) {
      LOGGER.debug("Received Sync from {}, updates: {}", sender, updates);
      processUpdates(updates, true/* spread gossip */);
    } else {
      LOGGER.debug("Received Sync from {}, no updates", sender);
    }
    MembershipData syncAckData = new MembershipData(membershipTable.asList(), config.getSyncGroup());
    Message syncAckMsg = Message.withData(syncAckData).qualifier(SYNC_ACK).build();
    transport.send(sender, syncAckMsg);
  }

  /**
   * Merges FD updates and processes them.
   */
  private void onFailureDetectorEvent(FailureDetectorEvent fdEvent) {
    List<MembershipRecord> updates = membershipTable.merge(fdEvent);
    if (!updates.isEmpty()) {
      LOGGER.debug("Received FD event {}, updates: {}", fdEvent, updates);
      processUpdates(updates, true/* spread gossip */);
    }
  }

  /**
   * Merges gossip's {@link MembershipData} (not spreading gossip further).
   */
  private void onMembershipGossip(MembershipData data) {
    List<MembershipRecord> updates = membershipTable.merge(data);
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
   * <li>publishes updates locally (see {@link #listenUpdates()})</li>
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
    Collection<Address> members = membershipTable.getTrustedOrSuspectedMembers();
    failureDetector.setMembers(members);
    gossipProtocol.setMembers(members);

    // Publish updates to cluster
    if (spreadGossip) {
      gossipProtocol.spread(Message.fromData(new MembershipData(updates, config.getSyncGroup())));
    }

    // Publish updates locally
    for (MembershipRecord update : updates) {
      subject.onNext(update);
    }

    // Check state transition
    for (final MembershipRecord member : updates) {
      LOGGER.debug("Member {} became {}", member.address(), member.status());
      switch (member.status()) {
        case SUSPECTED:
          // setup a schedule for suspected member
          if (removeMemberTasks.containsKey(member.id())) {
            break;
          }
          removeMemberTasks.put(member.id(), executor.schedule(() -> {
              LOGGER.debug("Time to remove SUSPECTED member={} from membership table", member);
              removeMemberTasks.remove(member.id());
              processUpdates(membershipTable.remove(member.id()), false/* spread gossip */);
            }, config.getMaxSuspectTime(), TimeUnit.MILLISECONDS));
          break;
        case TRUSTED:
          // clean schedule
          ScheduledFuture<?> future = removeMemberTasks.remove(member.id());
          if (future != null) {
            future.cancel(true);
          }
          break;
        case SHUTDOWN:
          executor.schedule(() -> {
              LOGGER.debug("Time to remove SHUTDOWN member={} from membership table", member.address());
              membershipTable.remove(member.id());
            }, config.getMaxShutdownTime(), TimeUnit.MILLISECONDS);
          break;
        default:
          // ignore
      }
    }
  }

  /**
   * Denoting fact that local member is getting gracefully shutdown. It will notify other members that going to be
   * stopped soon. After calling this method recommended to wait some reasonable amount of time to start spreading
   * information about leave before stopping server.
   */
  public void leave() {
    MembershipRecord leaveRecord = new MembershipRecord(localMember, SHUTDOWN);
    MembershipData msgData = new MembershipData(ImmutableList.of(leaveRecord), config.getSyncGroup());
    gossipProtocol.spread(Message.fromData(msgData));
  }

}
