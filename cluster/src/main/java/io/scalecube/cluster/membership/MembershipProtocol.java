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
import io.scalecube.transport.MessageHeaders;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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

import javax.annotation.Nullable;

public final class MembershipProtocol implements IMembershipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

  // qualifiers
  public static final String SYNC = "io.scalecube.cluster/membership/sync";
  public static final String SYNC_ACK = "io.scalecube.cluster/membership/syncAck";

  // filters
  private static final MessageHeaders.Filter SYNC_FILTER = new MessageHeaders.Filter(SYNC);
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

  private long period = 0;
  private final MembershipTable membershipTable = new MembershipTable();

  // Subscriptions

  private final Subject<MembershipRecord, MembershipRecord> subject = PublishSubject.<MembershipRecord>create()
      .toSerialized();

  private Subscriber<Message> onSyncRequestSubscriber;
  private Subscriber<FailureDetectorEvent> onFdEventSubscriber;
  private Subscriber<MembershipData> onGossipRequestSubscriber;


  // Scheduled

  private final Scheduler scheduler;
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> syncTask;
  private final Map<String, ScheduledFuture<?>> removeMemberTasks = new HashMap<>();

  private Function<Message, Void> onSyncAckFunction = new Function<Message, Void>() {
    @Override
    public Void apply(Message message) {
      onSyncAck(message);
      return null;
    }
  };

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
    this.seedMembers = cleanUpSeedMembers(config.getSeedMembers(), transport.address());
  }

  // Remove duplicates and local address
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers, Address localAddress) {
    Set<Address> seedMembersSet = new HashSet<>(seedMembers);
    seedMembersSet.remove(localAddress);
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

    // Listen to SYNC requests from joining/synchronizing members
    onSyncRequestSubscriber = Subscribers.create(new OnSyncRequestSubscriber());
    transport.listen().observeOn(scheduler)
        .filter(SYNC_FILTER)
        .filter(MembershipDataUtils.syncGroupFilter(config.getSyncGroup()))
        .subscribe(onSyncRequestSubscriber);

    // Listen to 'suspected/trusted' events from FailureDetector
    onFdEventSubscriber = Subscribers.create(new OnFdEventSubscriber());
    failureDetector.listenStatus().subscribe(onFdEventSubscriber);

    // Listen to 'membership' message from GossipProtocol
    onGossipRequestSubscriber = Subscribers.create(new OnGossipRequestAction());
    gossipProtocol.listen().observeOn(scheduler)
        .filter(GOSSIP_MEMBERSHIP_FILTER)
        .map(MembershipDataUtils.gossipFilterData(transport.address()))
        .subscribe(onGossipRequestSubscriber);

    // Conduct 'initialization phase': take seed addresses, send SYNC to all and get at least one SYNC_ACK from any
    // of them
    ListenableFuture<Void> startFuture;
    if (!seedMembers.isEmpty()) {
      LOGGER.debug("Initialization phase: making first Sync (wellknown_members={})", seedMembers);
      startFuture = doInitialSync(seedMembers);
    } else {
      startFuture = Futures.immediateFuture(null);
    }

    // Schedule 'running phase': select randomly single seed address, send SYNC and get SYNC_ACK
    if (!seedMembers.isEmpty()) {
      int syncTime = config.getSyncTime();
      syncTask = executor.scheduleWithFixedDelay(new SyncTask(), syncTime, syncTime, TimeUnit.MILLISECONDS);
    }
    return startFuture;
  }

  /**
   * Stops running cluster membership protocol and releases occupied resources.
   */
  public void stop() {
    // Stop accepting requests or events
    if (onSyncRequestSubscriber != null) {
      onSyncRequestSubscriber.unsubscribe();
    }
    if (onFdEventSubscriber != null) {
      onFdEventSubscriber.unsubscribe();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }

    // Stop sending sync
    if (syncTask != null) {
      syncTask.cancel(true);
    }

    // Shutdown executor
    executor.shutdownNow();

    // Clear suspected-schedule
    removeMemberTasks.clear();

    // Stop publishing events
    subject.onCompleted();
  }

  private ListenableFuture<Void> doInitialSync(final List<Address> seedMembers) {
    String cid = Long.toString(this.period);
    final SettableFuture<Message> syncResponseFuture = SettableFuture.create();

    transport.listen()
        .filter(syncAckFilter(cid))
        .filter(MembershipDataUtils.syncGroupFilter(config.getSyncGroup()))
        .take(1)
        .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS)
        .subscribe(new Action1<Message>() {
          @Override
          public void call(Message message) {
            syncResponseFuture.set(message);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            syncResponseFuture.setException(throwable);
          }
        });

    sendSync(seedMembers, cid);

    return Futures.catchingAsync(
        Futures.transform(syncResponseFuture, onSyncAckFunction),
        Throwable.class,
        new AsyncFunction<Throwable, Void>() {
          @Override
          public ListenableFuture<Void> apply(@Nullable Throwable throwable) throws Exception {
            LOGGER.info("Timeout getting initial SyncAck from seed members: {}", seedMembers);
            return Futures.immediateFuture(null);
          }
        });
  }

  private void doSync(final List<Address> members, Scheduler scheduler) {
    String cid = Long.toString(this.period);
    transport.listen()
        .filter(syncAckFilter(cid))
        .filter(MembershipDataUtils.syncGroupFilter(config.getSyncGroup()))
        .take(1)
        .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS, scheduler)
        .subscribe(Subscribers.create(new Action1<Message>() {
          @Override
          public void call(Message message) {
            onSyncAck(message);
          }
        }, new Action1<Throwable>() {
          @Override
          public void call(Throwable throwable) {
            LOGGER.info("Timeout getting SyncAck from members: {}", members);
          }
        }));

    sendSync(members, cid);
  }

  private void sendSync(List<Address> members, String cid) {
    MembershipData syncData = new MembershipData(membershipTable.asList(), config.getSyncGroup());
    final Message syncMsg = Message.withData(syncData).qualifier(SYNC).correlationId(cid).build();
    for (Address memberAddress : members) {
      transport.send(memberAddress, syncMsg);
    }
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

  private List<Address> selectRandomMembers(List<Address> members) {
    List<Address> list = new ArrayList<>(members);
    Collections.shuffle(list, ThreadLocalRandom.current());
    return ImmutableList.of(list.get(ThreadLocalRandom.current().nextInt(list.size())));
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
          removeMemberTasks.put(member.id(), executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOGGER.debug("Time to remove SUSPECTED member={} from membership table", member);
              removeMemberTasks.remove(member.id());
              processUpdates(membershipTable.remove(member.id()), false/* spread gossip */);
            }
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
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOGGER.debug("Time to remove SHUTDOWN member={} from membership table", member.address());
              membershipTable.remove(member.id());
            }
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

  private MessageHeaders.Filter syncAckFilter(String correlationId) {
    return new MessageHeaders.Filter(SYNC_ACK, correlationId);
  }

  /**
   * Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK.
   */
  private class OnSyncRequestSubscriber implements Action1<Message> {
    @Override
    public void call(Message message) {
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
      String correlationId = message.correlationId();
      MembershipData syncAckData = new MembershipData(membershipTable.asList(), config.getSyncGroup());
      Message syncAckMsg = Message.withData(syncAckData).qualifier(SYNC_ACK).correlationId(correlationId).build();
      transport.send(sender, syncAckMsg);
    }
  }

  /**
   * Merges FD updates and processes them.
   */
  private class OnFdEventSubscriber implements Action1<FailureDetectorEvent> {
    @Override
    public void call(FailureDetectorEvent fdEvent) {
      List<MembershipRecord> updates = membershipTable.merge(fdEvent);
      if (!updates.isEmpty()) {
        LOGGER.debug("Received FD event {}, updates: {}", fdEvent, updates);
        processUpdates(updates, true/* spread gossip */);
      }
    }
  }

  /**
   * Merges gossip's {@link MembershipData} (not spreading gossip further).
   */
  private class OnGossipRequestAction implements Action1<MembershipData> {
    @Override
    public void call(MembershipData data) {
      List<MembershipRecord> updates = membershipTable.merge(data);
      if (!updates.isEmpty()) {
        LOGGER.debug("Received gossip, updates: {}", updates);
        processUpdates(updates, false/* spread gossip */);
      }
    }
  }

  private class SyncTask implements Runnable {
    @Override
    public void run() {
      try {
        period++;
        // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
        List<Address> members = selectRandomMembers(seedMembers);
        LOGGER.debug("Running phase: making Sync (selected_members={}))", members);
        doSync(members, scheduler);
      } catch (Exception cause) {
        LOGGER.error("Unhandled exception: {}", cause, cause);
      }
    }
  }

}
