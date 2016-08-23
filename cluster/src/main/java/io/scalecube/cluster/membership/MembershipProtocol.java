package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.ClusterMemberStatus.SHUTDOWN;
import static io.scalecube.cluster.ClusterMemberStatus.TRUSTED;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.IFailureDetector;
import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageHeaders;
import io.scalecube.transport.Transport;
import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observable.ListenableFutureObservable;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
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
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class MembershipProtocol implements IMembershipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

  // qualifiers
  private static final String SYNC = "io.scalecube.cluster/membership/sync";
  private static final String SYNC_ACK = "io.scalecube.cluster/membership/syncAck";

  // filters
  private static final MessageHeaders.Filter SYNC_FILTER = new MessageHeaders.Filter(SYNC);
  private static final Func1<Message, Boolean> GOSSIP_MEMBERSHIP_FILTER = new Func1<Message, Boolean>() {
    @Override
    public Boolean call(Message message) {
      return message.data() != null && MembershipData.class.equals(message.data().getClass());
    }
  };

  // Injected

  private final String memberId;
  private final ITransport transport;
  private final MembershipConfig config;
  private IFailureDetector failureDetector;
  private IGossipProtocol gossipProtocol;
  private List<Address> seedMembers = new ArrayList<>();
  private Map<String, String> localMetadata = new HashMap<>();

  // State

  private AtomicInteger periodCounter = new AtomicInteger();
  private MembershipTable membershipTable = new MembershipTable();

  // Subscriptions

  private Subscriber<Message> onSyncRequestSubscriber;
  private Subscriber<FailureDetectorEvent> onFdEventSubscriber;
  private Subscriber<MembershipData> onGossipRequestSubscriber;
  private Subject<ClusterMember, ClusterMember> subject =
      new SerializedSubject<>(PublishSubject.<ClusterMember>create());

  // Scheduled

  private final Scheduler scheduler;
  private ScheduledFuture<?> syncTask;
  private final ScheduledExecutorService executor;
  private final Memoizer<String, ScheduledFuture<?>> removeMemberTasks = new Memoizer<>();

  private Function<Message, Void> onSyncAckFunction = new Function<Message, Void>() {
    @Nullable
    @Override
    public Void apply(@Nullable Message message) {
      onSyncAck(message);
      return null;
    }
  };

  public MembershipProtocol(String memberId, Transport transport, MembershipConfig config) {
    this.memberId = memberId;
    this.transport = transport;
    this.config = config;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("sc-membership-%s").setDaemon(true).build());
    this.scheduler = Schedulers.from(executor);
  }

  public void setFailureDetector(IFailureDetector failureDetector) {
    this.failureDetector = failureDetector;
  }

  IFailureDetector getFailureDetector() {
    return failureDetector;
  }

  public void setGossipProtocol(IGossipProtocol gossipProtocol) {
    this.gossipProtocol = gossipProtocol;
  }

  IGossipProtocol getGossipProtocol() {
    return gossipProtocol;
  }

  /**
   * Sets seed members from the formatted string. Members are separated by comma and have next format {@code host:port}.
   * If member format is incorrect it will be skipped.
   */
  public void setSeedMembers(String seedMembers) {
    List<Address> seedMembersList = new ArrayList<>();
    for (String token : new HashSet<>(Splitter.on(',').splitToList(seedMembers))) {
      if (token.length() != 0) {
        try {
          seedMembersList.add(Address.from(token.trim()));
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Skipped setting wellknown_member, caught: " + e);
        }
      }
    }
    setSeedMembers(seedMembersList);
  }

  public void setSeedMembers(Collection<Address> seedMembers) {
    // filter duplicates and local addresses
    Set<Address> seedMembersSet = new HashSet<>(seedMembers);
    seedMembersSet.remove(transport.address());
    this.seedMembers = new ArrayList<>(seedMembersSet);
  }

  public void setLocalMetadata(Map<String, String> localMetadata) {
    this.localMetadata = localMetadata;
  }

  @Override
  public Observable<ClusterMember> listenUpdates() {
    return subject;
  }

  @Override
  public List<ClusterMember> members() {
    return membershipTable.asList();
  }

  @Override
  public List<ClusterMember> otherMembers() {
    List<ClusterMember> members = membershipTable.asList();
    members.remove(localMember());
    return members;
  }

  @Override
  public ClusterMember member(String id) {
    checkArgument(!Strings.isNullOrEmpty(id), "Member id can't be null or empty");
    return membershipTable.get(id);
  }

  @Override
  public ClusterMember member(Address address) {
    checkArgument(address != null, "Member address can't be null or empty");
    return membershipTable.get(address);
  }

  @Override
  public ClusterMember localMember() {
    return membershipTable.get(memberId);
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public ListenableFuture<Void> start() {
    // Register itself initially before SYNC/SYNC_ACK
    ClusterMember localMember = new ClusterMember(memberId, transport.address(), TRUSTED, localMetadata);
    List<ClusterMember> updates = membershipTable.merge(localMember);
    processUpdates(updates, false/* spread gossip */);

    // Listen to SYNC requests from joining/synchronizing members
    onSyncRequestSubscriber = Subscribers.create(new OnSyncRequestSubscriber());
    transport.listen()
        .filter(SYNC_FILTER)
        .filter(MembershipDataUtils.syncGroupFilter(config.getSyncGroup()))
        .subscribe(onSyncRequestSubscriber);

    // Listen to 'suspected/trusted' events from FailureDetector
    onFdEventSubscriber = Subscribers.create(new OnFdEventSubscriber());
    failureDetector.listenStatus().subscribe(onFdEventSubscriber);

    // Listen to 'membership' message from GossipProtocol
    onGossipRequestSubscriber = Subscribers.create(new OnGossipRequestAction());
    gossipProtocol.listen()
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
    // stop accepting requests or events
    if (onSyncRequestSubscriber != null) {
      onSyncRequestSubscriber.unsubscribe();
    }
    if (onFdEventSubscriber != null) {
      onFdEventSubscriber.unsubscribe();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }
    // cancel algorithm
    if (syncTask != null) {
      syncTask.cancel(true);
    }
    // cancel suspected members schedule
    for (String memberId : removeMemberTasks.keySet()) {
      ScheduledFuture<?> future = removeMemberTasks.remove(memberId);
      if (future != null) {
        future.cancel(true);
      }
    }
    executor.shutdownNow(); // shutdown thread
    subject.onCompleted(); // stop publishing
  }

  private ListenableFuture<Void> doInitialSync(final List<Address> seedMembers) {
    String period = Integer.toString(periodCounter.incrementAndGet());
    ListenableFuture<Message> future = ListenableFutureObservable.to(
        transport.listen()
            .filter(syncAckFilter(period))
            .filter(MembershipDataUtils.syncGroupFilter(config.getSyncGroup()))
            .take(1)
            .timeout(config.getSyncTimeout(), TimeUnit.MILLISECONDS));

    sendSync(seedMembers, period);

    return Futures.withFallback(
        Futures.transform(future, onSyncAckFunction),
        new FutureFallback<Void>() {
          @Override
          public ListenableFuture<Void> create(@Nonnull Throwable throwable) throws Exception {
            LOGGER.info("Timeout getting initial SyncAck from seed members: {}", seedMembers);
            return Futures.immediateFuture(null);
          }
        });
  }

  private void doSync(final List<Address> members, Scheduler scheduler) {
    String period = Integer.toString(periodCounter.incrementAndGet());
    transport.listen()
        .filter(syncAckFilter(period))
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

    sendSync(members, period);
  }

  private void sendSync(List<Address> members, String period) {
    MembershipData syncData = new MembershipData(membershipTable.asList(), config.getSyncGroup());
    final Message syncMsg = Message.withData(syncData).qualifier(SYNC).correlationId(period).build();
    for (Address memberAddress : members) {
      transport.send(memberAddress, syncMsg);
    }
  }

  private void onSyncAck(Message message) {
    MembershipData data = message.data();
    MembershipData filteredData = MembershipDataUtils.filterData(transport.address(), data);
    Address sender = message.sender();
    List<ClusterMember> updates = membershipTable.merge(filteredData);
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
   * <li>if {@code spreadGossip} was set {@code true} -- converts {@code updates} to {@link MembershipData} and
   * send it to cluster via {@link #gossipProtocol}</li>
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
  private void processUpdates(List<ClusterMember> updates, boolean spreadGossip) {
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
    for (ClusterMember update : updates) {
      subject.onNext(update);
    }

    // Check state transition
    for (final ClusterMember member : updates) {
      LOGGER.debug("Member {} became {}", member.address(), member.status());
      switch (member.status()) {
        case SUSPECTED:
          failureDetector.suspect(member.address());
          // setup a schedule for suspected member
          removeMemberTasks.get(member.id(), new RemoveMemberTaskComputable());
          break;
        case TRUSTED:
          // clean schedule
          ScheduledFuture<?> future = removeMemberTasks.remove(member.id());
          if (future == null || future.cancel(true)) {
            failureDetector.trust(member.address());
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
    ClusterMember localMember = new ClusterMember(memberId, transport.address(), SHUTDOWN, localMetadata);
    MembershipData msgData = new MembershipData(ImmutableList.of(localMember), config.getSyncGroup());
    gossipProtocol.spread(Message.fromData(msgData));
  }

  @Override
  public boolean isLocalMember(ClusterMember member) {
    checkArgument(member != null);
    return this.localMember().address().equals(member.address());
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
      List<ClusterMember> updates = membershipTable.merge(filteredData);
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
      List<ClusterMember> updates = membershipTable.merge(fdEvent);
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
      List<ClusterMember> updates = membershipTable.merge(data);
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
        // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
        List<Address> members = selectRandomMembers(seedMembers);
        LOGGER.debug("Running phase: making Sync (selected_members={}))", members);
        doSync(members, scheduler);
      } catch (Exception cause) {
        LOGGER.error("Unhandled exception: {}", cause, cause);
      }
    }
  }

  private class RemoveMemberTaskComputable implements Computable<String, ScheduledFuture<?>> {
    @Override
    public ScheduledFuture<?> compute(final String memberId) {
      return executor.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            LOGGER.debug("Time to remove SUSPECTED member(id={}) from membership table", memberId);
            processUpdates(membershipTable.remove(memberId), false/* spread gossip */);
          } catch (Exception cause) {
            LOGGER.error("Unhandled exception: {}", cause, cause);
          } finally {
            removeMemberTasks.remove(memberId);
          }
        }
      }, config.getMaxSuspectTime(), TimeUnit.MILLISECONDS);
    }
  }
}
