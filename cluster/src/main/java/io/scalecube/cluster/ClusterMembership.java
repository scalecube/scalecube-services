package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.ClusterMemberStatus.SHUTDOWN;
import static io.scalecube.cluster.ClusterMemberStatus.TRUSTED;
import static io.scalecube.cluster.ClusterMembershipDataUtils.gossipFilterData;
import static io.scalecube.cluster.ClusterMembershipDataUtils.syncGroupFilter;

import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.fdetector.IFailureDetector;
import io.scalecube.cluster.gossip.IManagedGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageHeaders;
import io.scalecube.transport.Transport;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ClusterMembership implements IClusterMembership {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembership.class);

  // qualifiers
  private static final String SYNC = "io.scalecube.cluster/membership/sync";
  private static final String SYNC_ACK = "io.scalecube.cluster/membership/syncAck";

  // filters
  private static final MessageHeaders.Filter SYNC_FILTER = new MessageHeaders.Filter(SYNC);
  private static final Func1<Message, Boolean> GOSSIP_MEMBERSHIP_FILTER = new Func1<Message, Boolean>() {
    @Override
    public Boolean call(Message message) {
      Object data = message.data();
      return data != null && ClusterMembershipData.class.equals(data.getClass());
    }
  };

  private IFailureDetector failureDetector;
  private IManagedGossipProtocol gossipProtocol;
  private int syncTime = 10 * 1000;
  private int syncTimeout = 3 * 1000;
  private int maxSuspectTime = 60 * 1000;
  private int maxShutdownTime = 60 * 1000;
  private String syncGroup = "default";
  private List<Address> seedMembers = new ArrayList<>();
  private final ITransport transport;
  private final String memberId;
  private final Scheduler scheduler;
  private volatile Subscription cmTask;
  private TickingTimer timer;
  private AtomicInteger periodNbr = new AtomicInteger();
  private ClusterMembershipTable membership = new ClusterMembershipTable();
  @SuppressWarnings("unchecked")
  private Subject<ClusterMember, ClusterMember> subject = new SerializedSubject(PublishSubject.create());
  private Map<String, String> localMetadata = new HashMap<>();

  /** Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK. */
  private Subscriber<Message> onSyncSubscriber = Subscribers.create(new Action1<Message>() {
    @Override
    public void call(Message message) {
      ClusterMembershipData data = message.data();
      ClusterMembershipData filteredData = ClusterMembershipDataUtils.filterData(transport.localAddress(), data);
      List<ClusterMember> updates = membership.merge(filteredData);
      Address sender = message.sender();
      if (!updates.isEmpty()) {
        LOGGER.debug("Received Sync from {}, updates: {}", sender, updates);
        processUpdates(updates, true/* spread gossip */);
      } else {
        LOGGER.debug("Received Sync from {}, no updates", sender);
      }
      String correlationId = message.correlationId();
      ClusterMembershipData syncAckData = new ClusterMembershipData(membership.asList(), syncGroup);
      Message syncAckMsg = Message.withData(syncAckData).qualifier(SYNC_ACK).correlationId(correlationId).build();
      transport.send(sender, syncAckMsg);
    }
  });

  /** Merges FD updates and processes them. */
  private Subscriber<FailureDetectorEvent> onFdSubscriber = Subscribers.create(new Action1<FailureDetectorEvent>() {
    @Override
    public void call(FailureDetectorEvent input) {
      List<ClusterMember> updates = membership.merge(input);
      if (!updates.isEmpty()) {
        LOGGER.debug("Received FD event {}, updates: {}", input, updates);
        processUpdates(updates, true/* spread gossip */);
      }
    }
  });

  /**
   * Merges gossip's {@link ClusterMembershipData} (not spreading gossip further).
   */
  private Subscriber<ClusterMembershipData> onGossipSubscriber = Subscribers
      .create(new Action1<ClusterMembershipData>() {
        @Override
        public void call(ClusterMembershipData data) {
          List<ClusterMember> updates = membership.merge(data);
          if (!updates.isEmpty()) {
            LOGGER.debug("Received gossip, updates: {}", updates);
            processUpdates(updates, false/* spread gossip */);
          }
        }
      });
  private Function<Message, Void> onSyncAckFunction = new Function<Message, Void>() {
    @Nullable
    @Override
    public Void apply(@Nullable Message message) {
      onSyncAck(message);
      return null;
    }
  };

  ClusterMembership(String memberId, Transport transport) {
    this.memberId = memberId;
    this.transport = transport;
    this.scheduler = Schedulers.from(transport.getWorkerGroup());
  }

  public void setFailureDetector(IFailureDetector failureDetector) {
    this.failureDetector = failureDetector;
  }

  public void setGossipProtocol(IManagedGossipProtocol gossipProtocol) {
    this.gossipProtocol = gossipProtocol;
  }

  public void setSyncTime(int syncTime) {
    this.syncTime = syncTime;
  }

  public void setSyncTimeout(int syncTimeout) {
    this.syncTimeout = syncTimeout;
  }

  public void setMaxSuspectTime(int maxSuspectTime) {
    this.maxSuspectTime = maxSuspectTime;
  }

  public void setMaxShutdownTime(int maxShutdownTime) {
    this.maxShutdownTime = maxShutdownTime;
  }

  public void setSyncGroup(String syncGroup) {
    this.syncGroup = syncGroup;
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
    seedMembersSet.remove(transport.localAddress());
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
    return membership.asList();
  }

  @Override
  public ClusterMember member(String id) {
    checkArgument(!Strings.isNullOrEmpty(id), "Member id can't be null or empty");
    return membership.get(id);
  }

  @Override
  public ClusterMember member(Address address) {
    checkArgument(address != null, "Member address can't be null or empty");
    return membership.get(address);
  }

  @Override
  public ClusterMember localMember() {
    return membership.get(memberId);
  }

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  public ListenableFuture<Void> start() {
    // Start timer
    timer = new TickingTimer();
    timer.start();

    // Register itself initially before SYNC/SYNC_ACK
    ClusterMember localMember = new ClusterMember(memberId, transport.localAddress(), TRUSTED, localMetadata);
    List<ClusterMember> updates = membership.merge(localMember);
    processUpdates(updates, false/* spread gossip */);

    // Listen to SYNC requests from joining/synchronizing members
    transport.listen()
        .filter(syncFilter())
        .filter(syncGroupFilter(syncGroup))
        .subscribe(onSyncSubscriber);

    // Listen to 'suspected/trusted' events from FailureDetector
    failureDetector.listenStatus().subscribe(onFdSubscriber);

    // Listen to 'membership' message from GossipProtocol
    gossipProtocol.listen().filter(GOSSIP_MEMBERSHIP_FILTER).map(gossipFilterData(transport.localAddress()))
        .subscribe(onGossipSubscriber);

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
      cmTask = scheduler.createWorker().schedulePeriodically(new Action0() {
        @Override
        public void call() {
          try {
            // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
            List<Address> members = selectRandomMembers(seedMembers);
            LOGGER.debug("Running phase: making Sync (selected_members={}))", members);
            doSync(members, scheduler);
          } catch (Exception e) {
            LOGGER.error("Unhandled exception: {}", e, e);
          }
        }
      }, syncTime, syncTime, TimeUnit.MILLISECONDS);
    }
    return startFuture;
  }

  /**
   * Stops running cluster membership protocol and releases occupied resources.
   */
  public void stop() {
    if (cmTask != null) {
      cmTask.unsubscribe();
    }
    subject.onCompleted();
    onGossipSubscriber.unsubscribe();
    onSyncSubscriber.unsubscribe();
    onFdSubscriber.unsubscribe();
    timer.stop();
  }

  private ListenableFuture<Void> doInitialSync(final List<Address> seedMembers) {
    String period = Integer.toString(periodNbr.incrementAndGet());
    ListenableFuture<Message> future = ListenableFutureObservable.to(
        transport.listen()
            .filter(syncAckFilter(period))
            .filter(syncGroupFilter(syncGroup))
            .take(1)
            .timeout(syncTimeout, TimeUnit.MILLISECONDS));

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
    String period = Integer.toString(periodNbr.incrementAndGet());
    transport.listen()
        .filter(syncAckFilter(period))
        .filter(syncGroupFilter(syncGroup))
        .take(1)
        .timeout(syncTimeout, TimeUnit.MILLISECONDS, scheduler)
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
    ClusterMembershipData syncData = new ClusterMembershipData(membership.asList(), syncGroup);
    final Message syncMsg = Message.withData(syncData).qualifier(SYNC).correlationId(period).build();
    for (Address memberAddress : members) {
      transport.send(memberAddress, syncMsg);
    }
  }

  private void onSyncAck(Message message) {
    ClusterMembershipData data = message.data();
    ClusterMembershipData filteredData = ClusterMembershipDataUtils.filterData(transport.localAddress(), data);
    Address sender = message.sender();
    List<ClusterMember> updates = membership.merge(filteredData);
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
   * <li>if {@code spreadGossip} was set {@code true} -- converts {@code updates} to {@link ClusterMembershipData} and
   * send it to cluster via {@link #gossipProtocol}</li>
   * <li>publishes updates locally (see {@link #listenUpdates()})</li>
   * <li>iterates on {@code updates}, if {@code update} become {@code SUSPECTED} -- schedules a timer (
   * {@link #maxSuspectTime}) to remove the member (on {@code TRUSTED} -- cancels the timer)</li>
   * <li>iterates on {@code updates}, if {@code update} become {@code SHUTDOWN} -- schedules a timer (
   * {@link #maxShutdownTime}) to remove the member</li>
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
    Collection<Address> members = membership.getTrustedOrSuspectedMembers();
    failureDetector.setClusterMembers(members);
    gossipProtocol.setClusterMembers(members);

    // Publish updates to cluster
    if (spreadGossip) {
      gossipProtocol.spread(Message.fromData(new ClusterMembershipData(updates, syncGroup)));
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
          timer.schedule(member.id(), new Runnable() {
            @Override
            public void run() {
              LOGGER.debug("Time to remove SUSPECTED member={} from membership", member.address());
              processUpdates(membership.remove(member.id()), false/* spread gossip */);
            }
          }, maxSuspectTime, TimeUnit.MILLISECONDS);
          break;
        case TRUSTED:
          failureDetector.trust(member.address());
          timer.cancel(member.id());
          break;
        case SHUTDOWN:
          timer.schedule(new Runnable() {
            @Override
            public void run() {
              LOGGER.debug("Time to remove SHUTDOWN member={} from membership", member.address());
              membership.remove(member.id());
            }
          }, maxShutdownTime, TimeUnit.MILLISECONDS);
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
    ClusterMember localMember = new ClusterMember(memberId, transport.localAddress(), SHUTDOWN, localMetadata);
    gossipProtocol.spread(Message.fromData(new ClusterMembershipData(ImmutableList.of(localMember), syncGroup)));
  }

  @Override
  public boolean isLocalMember(ClusterMember member) {
    checkArgument(member != null);
    return this.localMember().address().equals(member.address());
  }

  private MessageHeaders.Filter syncFilter() {
    return SYNC_FILTER;
  }

  private MessageHeaders.Filter syncAckFilter(String correlationId) {
    return new MessageHeaders.Filter(SYNC_ACK, correlationId);
  }
}
