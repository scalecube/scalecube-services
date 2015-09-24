package io.servicefabric.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.Message;

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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class GossipProtocol implements IGossipProtocol, IManagedGossipProtocol {
  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

  private ITransport transport;
  @SuppressWarnings("unchecked")
  private Subject<Message, Message> subject = new SerializedSubject(PublishSubject.create());
  private Map<String, GossipLocalState> gossipsMap = Maps.newHashMap();
  private volatile List<TransportEndpoint> members = new ArrayList<>();
  private final TransportEndpoint localEndpoint;
  private final ScheduledExecutorService executor;
  private AtomicLong counter = new AtomicLong(0);
  private Queue<GossipTask> gossipsQueue = new ConcurrentLinkedQueue<>();
  private int maxGossipSent = 2;
  private int gossipTime = 300;
  private int maxEndpointsToSelect = 3;
  private long period = 0;
  private volatile int factor = 1;
  private Subscriber<Message> onGossipRequestSubscriber;
  private ScheduledFuture<?> executorTask;

  public GossipProtocol(TransportEndpoint localEndpoint) {
    this.localEndpoint = localEndpoint;
    this.executor = createDedicatedScheduledExecutor();
  }

  public GossipProtocol(TransportEndpoint localEndpoint, ScheduledExecutorService executor) {
    this.localEndpoint = localEndpoint;
    this.executor = executor;
  }

  private ScheduledExecutorService createDedicatedScheduledExecutor() {
    return Executors.newScheduledThreadPool(1, createThreadFactory("servicefabric-gossip-scheduled-%s"));
  }

  private ThreadFactory createThreadFactory(String namingFormat) {
    return new ThreadFactoryBuilder().setNameFormat(namingFormat)
        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable ex) {
            LOGGER.error("Unhandled exception: {}", ex, ex);
          }
        }).setDaemon(true).build();
  }

  public void setMaxGossipSent(int maxGossipSent) {
    this.maxGossipSent = maxGossipSent;
  }

  public void setGossipTime(int gossipTime) {
    this.gossipTime = gossipTime;
  }

  public int getGossipTime() {
    return gossipTime;
  }

  public void setMaxEndpointsToSelect(int maxEndpointsToSelect) {
    this.maxEndpointsToSelect = maxEndpointsToSelect;
  }

  @Override
  public void setClusterEndpoints(Collection<TransportEndpoint> members) {
    Set<TransportEndpoint> set = new HashSet<>(members);
    set.remove(localEndpoint);
    List<TransportEndpoint> list = new ArrayList<>(set);
    Collections.shuffle(list);
    this.members = list;
    this.factor = 32 - Integer.numberOfLeadingZeros(list.size() + 1);
    LOGGER.debug("Set cluster members: {}", this.members);
  }

  public void setTransport(ITransport transport) {
    this.transport = transport;
  }

  public ITransport getTransport() {
    return transport;
  }

  @Override
  public void start() {
    onGossipRequestSubscriber = Subscribers.create(new OnGossipRequestAction(gossipsQueue));
    transport.listen().filter(new GossipMessageFilter()).subscribe(onGossipRequestSubscriber);
    executorTask =
        executor.scheduleWithFixedDelay(new GossipProtocolRunnable(), gossipTime, gossipTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    subject.onCompleted();
    if (executorTask != null) {
      executorTask.cancel(true);
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.unsubscribe();
    }
  }

  @Override
  public void spread(Message message) {
    String id = generateGossipId();
    Gossip gossip = new Gossip(id, message);
    gossipsQueue.offer(new GossipTask(gossip, localEndpoint));
  }

  @Override
  public Observable<Message> listen() {
    return subject;
  }

  private Collection<GossipLocalState> processGossipQueue() {
    while (!gossipsQueue.isEmpty()) {
      GossipTask gossipTask = gossipsQueue.poll();
      Gossip gossip = gossipTask.getGossip();
      TransportEndpoint endpoint = gossipTask.getOrigin();
      GossipLocalState gossipLocalState = gossipsMap.get(gossip.getGossipId());
      if (gossipLocalState == null) {
        boolean isEndpointRemote = !endpoint.equals(localEndpoint);
        LOGGER.debug("Saved new_" + (isEndpointRemote ? "remote" : "local") + " {}", gossip);
        gossipLocalState = GossipLocalState.create(gossip, endpoint, period);
        gossipsMap.put(gossip.getGossipId(), gossipLocalState);
        if (isEndpointRemote) {
          subject.onNext(gossip.getMessage());
        }
      } else {
        gossipLocalState.addMember(endpoint);
      }
    }
    return gossipsMap.values();
  }

  private void sendGossips(List<TransportEndpoint> members, Collection<GossipLocalState> gossips, Integer factor) {
    if (gossips.isEmpty()) {
      return;
    }
    if (members.isEmpty()) {
      return;
    }
    if (period % members.size() == 0) {
      Collections.shuffle(members, ThreadLocalRandom.current());
    }
    for (int i = 0; i < Math.min(maxEndpointsToSelect, members.size()); i++) {
      TransportEndpoint transportEndpoint = getNextRandom(members, maxEndpointsToSelect, i);
      // Filter only gossips which should be sent to chosen clusterEndpoint
      GossipSendPredicate predicate = new GossipSendPredicate(transportEndpoint, maxGossipSent * factor);
      Collection<GossipLocalState> gossipLocalStateNeedSend = filter(gossips, predicate);
      if (!gossipLocalStateNeedSend.isEmpty()) {
        // Transform to actual gossip with incrementing sent count
        List<Gossip> gossipToSend =
            newArrayList(transform(gossipLocalStateNeedSend, new GossipDataToGossipWithIncrement()));
        transport.send(transportEndpoint, new Message(new GossipRequest(gossipToSend)));
      }
    }
  }

  // Remove old gossips
  private void sweep(Collection<GossipLocalState> values, int factor) {
    Collection<GossipLocalState> filter = newHashSet(filter(values, new GossipSweepPredicate(period, factor * 10)));
    for (GossipLocalState gossipLocalState : filter) {
      gossipsMap.remove(gossipLocalState.gossip().getGossipId());
      LOGGER.debug("Removed {}", gossipLocalState);
    }
  }

  private String generateGossipId() {
    return localEndpoint.id() + "_" + counter.getAndIncrement();
  }

  private TransportEndpoint getNextRandom(List<TransportEndpoint> members, int endpointCount, int count) {
    return members.get((int) (((period * endpointCount + count) & Integer.MAX_VALUE) % members.size()));

  }

  // gossip functions.
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
      TransportEndpoint transportEndpoint = message.sender();
      for (Gossip gossip : gossipRequest.getGossipList()) {
        queue.offer(new GossipTask(gossip, transportEndpoint));
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
    private final TransportEndpoint endpoint;
    private final int maxCounter;

    GossipSendPredicate(TransportEndpoint endpoint, int maxCounter) {
      this.endpoint = endpoint;
      this.maxCounter = maxCounter;
    }

    @Override
    public boolean apply(GossipLocalState input) {
      return !input.containsMember(endpoint) && input.getSent() < maxCounter;
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
    private final TransportEndpoint origin;

    GossipTask(Gossip gossip, TransportEndpoint origin) {
      this.gossip = gossip;
      this.origin = origin;
    }

    public Gossip getGossip() {
      return gossip;
    }

    public TransportEndpoint getOrigin() {
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
      return "GossipTask{" + "gossip=" + gossip + ", clusterEndpoint=" + origin + '}';
    }
  }

  class GossipProtocolRunnable implements Runnable {
    @Override
    public void run() {
      try {
        period++;
        Collection<GossipLocalState> gossips = processGossipQueue();
        List<TransportEndpoint> members = GossipProtocol.this.members;
        int factor = GossipProtocol.this.factor;
        sendGossips(members, gossips, factor);
        sweep(gossips, factor);
      } catch (Exception e) {
        LOGGER.error("Unhandled exception: {}", e, e);
      }
    }
  }
}
