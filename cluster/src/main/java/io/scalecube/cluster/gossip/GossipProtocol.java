package io.scalecube.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import io.scalecube.transport.Address;
import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class GossipProtocol implements IGossipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

  // Injected

  private final String memberId;
  private final ITransport transport;
  private final GossipConfig config;

  // State

  private AtomicLong counter = new AtomicLong(0);
  private Queue<GossipTask> gossipsQueue = new ConcurrentLinkedQueue<>();
  private long period = 0;
  private volatile int factor = 1;
  private Map<String, GossipLocalState> gossipsMap = Maps.newHashMap();
  private volatile List<Address> members = new ArrayList<>();

  // Subscriptions

  private Subscriber<Message> onGossipRequestSubscriber;
  private Subject<Message, Message> subject = PublishSubject.<Message>create().toSerialized();

  // Scheduled

  private ScheduledFuture<?> executorTask;
  private final ScheduledExecutorService executor;

  /**
   * Creates new instance of gossip protocol with given memberId, transport and default settings.
   *
   * @param memberId id of current member
   * @param transport transport
   */
  public GossipProtocol(String memberId, ITransport transport) {
    this(memberId, transport, GossipConfig.DEFAULT);
  }

  /**
   * Creates new instance of gossip protocol with given memberId, transport and settings.
   *
   * @param memberId id of current member
   * @param transport transport
   * @param config gossip protocol settings
   */
  public GossipProtocol(String memberId, ITransport transport, GossipConfig config) {
    this.memberId = memberId;
    this.transport = transport;
    this.config = config;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("sc-gossip-%s").setDaemon(true).build());
  }

  @Override
  public void setMembers(Collection<Address> members) {
    Set<Address> remoteMembers = new HashSet<>(members);
    remoteMembers.remove(transport.address());
    List<Address> list = new ArrayList<>(remoteMembers);
    Collections.shuffle(list);
    this.members = list;
    this.factor = 32 - Integer.numberOfLeadingZeros(list.size() + 1);
    LOGGER.debug("Set cluster members[{}]: {}", this.members.size(), this.members);
  }

  public ITransport getTransport() {
    return transport;
  }

  @Override
  public void start() {
    onGossipRequestSubscriber = Subscribers.create(new OnGossipRequestAction(gossipsQueue));
    transport.listen().filter(new GossipMessageFilter()).subscribe(onGossipRequestSubscriber);

    int gossipTime = config.getGossipTime();
    executorTask =
        executor.scheduleWithFixedDelay(new GossipProtocolRunnable(), gossipTime, gossipTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    // stop accepting requests
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }
    // cancel algorithm
    if (executorTask != null) {
      executorTask.cancel(true);
    }
    executor.shutdownNow(); // shutdown thread
    subject.onCompleted(); // stop publishing
  }

  @Override
  public void spread(Message message) {
    String gossipId = generateGossipId();
    Gossip gossip = new Gossip(gossipId, message);
    GossipTask gossipTask = new GossipTask(gossip, transport.address());
    gossipsQueue.offer(gossipTask);
  }

  @Override
  public Observable<Message> listen() {
    return subject;
  }

  private Collection<GossipLocalState> processGossipQueue() {
    while (!gossipsQueue.isEmpty()) {
      GossipTask gossipTask = gossipsQueue.poll();
      Gossip gossip = gossipTask.getGossip();
      Address origin = gossipTask.getOrigin();
      GossipLocalState gossipLocalState = gossipsMap.get(gossip.getGossipId());
      if (gossipLocalState == null) {
        boolean isRemote = !origin.equals(transport.address());
        LOGGER.debug("Saved new_" + (isRemote ? "remote" : "local") + " {}", gossip);
        gossipLocalState = GossipLocalState.create(gossip, origin, period);
        gossipsMap.put(gossip.getGossipId(), gossipLocalState);
        if (isRemote) {
          subject.onNext(gossip.getMessage());
        }
      } else {
        gossipLocalState.addMember(origin);
      }
    }
    return gossipsMap.values();
  }

  private void sendGossips(List<Address> members, Collection<GossipLocalState> gossips, Integer factor) {
    if (gossips.isEmpty()) {
      return;
    }
    if (members.isEmpty()) {
      return;
    }
    if (period % members.size() == 0) {
      Collections.shuffle(members, ThreadLocalRandom.current());
    }
    int maxMembersToSelect = config.getMaxMembersToSelect();
    for (int i = 0; i < Math.min(maxMembersToSelect, members.size()); i++) {
      Address address = getNextRandom(members, maxMembersToSelect, i);
      // Filter only gossips which should be sent to chosen address
      GossipSendPredicate predicate = new GossipSendPredicate(address, config.getMaxGossipSent() * factor);
      Collection<GossipLocalState> gossipLocalStateNeedSend = filter(gossips, predicate);
      if (!gossipLocalStateNeedSend.isEmpty()) {
        // Transform to actual gossip with incrementing sent count
        List<Gossip> gossipToSend =
            newArrayList(transform(gossipLocalStateNeedSend, new GossipDataToGossipWithIncrement()));
        transport.send(address, Message.fromData(new GossipRequest(gossipToSend)));
      }
    }
  }

  private void sweep(Collection<GossipLocalState> values, int factor) {
    Collection<GossipLocalState> filter = newHashSet(filter(values, new GossipSweepPredicate(period, factor * 10)));
    for (GossipLocalState gossipLocalState : filter) {
      gossipsMap.remove(gossipLocalState.gossip().getGossipId());
      LOGGER.debug("Removed {}", gossipLocalState);
    }
  }

  private String generateGossipId() {
    return memberId + "_" + counter.getAndIncrement();
  }

  private Address getNextRandom(List<Address> members, int maxMembersToSelect, int count) {
    return members.get((int) (((period * maxMembersToSelect + count) & Integer.MAX_VALUE) % members.size()));

  }

  static class GossipMessageFilter implements Func1<Message, Boolean> {
    @Override
    public Boolean call(Message message) {
      Object data = message.data();
      return data != null && GossipRequest.class.equals(data.getClass());
    }
  }

  static class OnGossipRequestAction implements Action1<Message> {
    private Queue<GossipTask> queue;

    OnGossipRequestAction(Queue<GossipTask> queue) {
      checkArgument(queue != null);
      this.queue = queue;
    }

    @Override
    public void call(Message message) {
      GossipRequest gossipRequest = message.data();
      Address sender = message.sender();
      for (Gossip gossip : gossipRequest.getGossipList()) {
        queue.offer(new GossipTask(gossip, sender));
      }
    }
  }

  static class GossipDataToGossipWithIncrement implements Function<GossipLocalState, Gossip> {
    @Override
    public Gossip apply(GossipLocalState input) {
      // side effect
      input.incrementSend();
      return input.gossip();
    }
  }

  static class GossipSendPredicate implements Predicate<GossipLocalState> {
    private final Address address;
    private final int maxCounter;

    GossipSendPredicate(Address address, int maxCounter) {
      this.address = address;
      this.maxCounter = maxCounter;
    }

    @Override
    public boolean apply(GossipLocalState input) {
      return !input.containsMember(address) && input.getSent() < maxCounter;
    }
  }

  static class GossipSweepPredicate implements Predicate<GossipLocalState> {
    private long currentPeriod;
    private int maxPeriods;

    GossipSweepPredicate(long currentPeriod, int maxPeriods) {
      this.currentPeriod = currentPeriod;
      this.maxPeriods = maxPeriods;
    }

    @Override
    public boolean apply(GossipLocalState input) {
      return currentPeriod - (input.getPeriod() + maxPeriods) > 0;
    }
  }

  static class GossipTask {
    private final Gossip gossip;
    private final Address origin;

    GossipTask(Gossip gossip, Address origin) {
      this.gossip = gossip;
      this.origin = origin;
    }

    public Gossip getGossip() {
      return gossip;
    }

    public Address getOrigin() {
      return origin;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      GossipTask that = (GossipTask) other;
      return Objects.equals(gossip, that.gossip) && Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
      return Objects.hash(gossip, origin);
    }

    @Override
    public String toString() {
      return "GossipTask{" + "gossip=" + gossip + ", origin=" + origin + '}';
    }
  }

  private class GossipProtocolRunnable implements Runnable {
    @Override
    public void run() {
      try {
        period++;
        Collection<GossipLocalState> gossips = processGossipQueue();
        List<Address> members = GossipProtocol.this.members;
        int factor = GossipProtocol.this.factor;
        sendGossips(members, gossips, factor);
        sweep(gossips, factor);
      } catch (Exception cause) {
        LOGGER.error("Unhandled exception: {}", cause, cause);
      }
    }
  }
}
