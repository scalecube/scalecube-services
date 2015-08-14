package io.servicefabric.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.servicefabric.transport.ITransportTypeRegistry;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.SchemaCache;
import io.servicefabric.transport.TransportTypeRegistry;
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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportMessage;

public final class GossipProtocol implements IGossipProtocol, IGossipProtocolSpi {
	private final static Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

	private ITransport transport;
	private Subject<Gossip, Gossip> subject = new SerializedSubject(PublishSubject.create());
	private Map<String, GossipLocalState> gossipsMap = Maps.newHashMap();
	private volatile List<ClusterEndpoint> members = new ArrayList<>();
	private final ClusterEndpoint localEndpoint;
	private final ScheduledExecutorService executor;
	private AtomicLong counter = new AtomicLong(0);
	private Queue<GossipTask> gossipsQueue = new ConcurrentLinkedQueue<>();
	private int maxGossipSent = 2;
	private int gossipTime = 300;
	private int maxEndpointsToSelect = 3;
	private long period = 0;
	private volatile int factor = 1;
	private Subscriber<TransportMessage> onGossipRequestSubscriber;
	private ScheduledFuture<?> executorTask;

	public GossipProtocol(ClusterEndpoint localEndpoint) {
		this.localEndpoint = localEndpoint;
		this.executor = createDedicatedScheduledExecutor();
	}

	public GossipProtocol(ClusterEndpoint localEndpoint, ScheduledExecutorService executor) {
		this.localEndpoint = localEndpoint;
		this.executor = executor;
	}

	private  ScheduledExecutorService createDedicatedScheduledExecutor() {
		return Executors.newScheduledThreadPool(1, createThreadFactory("servicefabric-gossip-scheduled-%s"));
	}

	private ThreadFactory createThreadFactory(String namingFormat) {
		return new ThreadFactoryBuilder()
				.setNameFormat(namingFormat)
				.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
					@Override
					public void uncaughtException(Thread t, Throwable e) {
						LOGGER.error("Unhandled exception: {}", e, e);
					}
				}).setDaemon(true).build();
	}

	public void setMaxGossipSent(int maxGossipSent) {
		this.maxGossipSent = maxGossipSent;
	}

	public void setGossipTime(int gossipTime) {
		this.gossipTime = gossipTime;
	}

	public void setMaxEndpointsToSelect(int maxEndpointsToSelect) {
		this.maxEndpointsToSelect = maxEndpointsToSelect;
	}

	@Override
	public void setClusterMembers(Collection<ClusterEndpoint> members) {
		Set<ClusterEndpoint> set = new HashSet<>(members);
		set.remove(localEndpoint);
		List<ClusterEndpoint> list = new ArrayList<>(set);
		Collections.shuffle(list);
		this.members = list;
		this.factor = 32 - Integer.numberOfLeadingZeros(list.size() + 1);
		LOGGER.debug("Set cluster members: {}", this.members);
	}

	public void setTransport(ITransport transport) {
		this.transport = transport;
	}

	public ClusterEndpoint getLocalEndpoint() {
		return localEndpoint;
	}

	public ITransport getTransport() {
		return transport;
	}

	@Override
	public void start() {
		// Register data types
		ITransportTypeRegistry typeRegistry = TransportTypeRegistry.getInstance();
		typeRegistry.registerType(GossipQualifiers.QUALIFIER, GossipRequest.class);
		SchemaCache.registerCustomSchema(Gossip.class, new GossipSchema(typeRegistry));

		onGossipRequestSubscriber = Subscribers.create(new OnGossipRequestAction(gossipsQueue));
		transport.listen().filter(new GossipMessageFilter()).subscribe(onGossipRequestSubscriber);
		executorTask = executor.scheduleWithFixedDelay(new GossipProtocolRunnable(), gossipTime, gossipTime, TimeUnit.MILLISECONDS);
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
	public void spread(String qualifier, Object data) {
		String id = generateId();
		Gossip gossip = new Gossip(id, qualifier, data);
		gossipsQueue.offer(new GossipTask(gossip, localEndpoint));
	}

	@Override
	public Observable<Gossip> listen() {
		return subject;
	}

	private Collection<GossipLocalState> processGossipQueue() {
		while (!gossipsQueue.isEmpty()) {
			GossipTask gossipTask = gossipsQueue.poll();
			Gossip gossip = gossipTask.getGossip();
			ClusterEndpoint endpoint = gossipTask.getOrigin();
			GossipLocalState gossipLocalState = gossipsMap.get(gossip.getGossipId());
			if (gossipLocalState == null) {
				boolean isEndpointRemote = !endpoint.equals(localEndpoint);
				LOGGER.debug("Saved new_" + (isEndpointRemote ? "remote" : "local") + " {}", gossip);
				gossipLocalState = GossipLocalState.create(gossip, endpoint, period);
				gossipsMap.put(gossip.getGossipId(), gossipLocalState);
				if (isEndpointRemote) {
					subject.onNext(gossip);
				}
			} else {
				gossipLocalState.addMember(endpoint);
			}
		}
		return gossipsMap.values();
	}

	private void sendGossips(List<ClusterEndpoint> members, Collection<GossipLocalState> gossips, Integer factor) {
		if (gossips.isEmpty())
			return;
		if (members.isEmpty())
			return;
		if (period % members.size() == 0)
			Collections.shuffle(members, ThreadLocalRandom.current());
		for (int i = 0; i < Math.min(maxEndpointsToSelect, members.size()); i++) {
			ClusterEndpoint clusterEndpoint = getNextRandom(members, maxEndpointsToSelect, i);
			//Filter only gossips which should be sent to chosen clusterEndpoint
			GossipSendPredicate predicate = new GossipSendPredicate(clusterEndpoint, maxGossipSent * factor);
			Collection<GossipLocalState> gossipLocalStateNeedSend = filter(gossips, predicate);
			if (!gossipLocalStateNeedSend.isEmpty()) {
				//Transform to actual gossip with incrementing sent count
				List<Gossip> gossipToSend = newArrayList(transform(gossipLocalStateNeedSend, new GossipDataToGossipWithIncrement()));
				transport.to(clusterEndpoint.endpoint())
						.send(new Message(GossipQualifiers.QUALIFIER, new GossipRequest(gossipToSend)), null);
			}
		}
	}

	//Remove old gossips
	private void sweep(Collection<GossipLocalState> values, int factor) {
		Collection<GossipLocalState> filter = newHashSet(filter(values, new GossipSweepPredicate(period, factor * 10)));
		for (GossipLocalState gossipLocalState : filter) {
			gossipsMap.remove(gossipLocalState.gossip().getGossipId());
			LOGGER.debug("Removed {}", gossipLocalState);
		}
	}

	private String generateId() {
		return localEndpoint.endpointId() + "_" + counter.getAndIncrement();
	}

	private ClusterEndpoint getNextRandom(List<ClusterEndpoint> members, int endpointCount, int i) {
		return members.get((int) (((period * endpointCount + i) & Integer.MAX_VALUE) % members.size()));

	}

	//gossip functions.
	static class GossipMessageFilter implements Func1<TransportMessage, Boolean> {
		@Override
		public Boolean call(TransportMessage transportMessage) {
			return transportMessage.message().qualifier().equalsIgnoreCase(GossipQualifiers.QUALIFIER);
		}
	}

	static class OnGossipRequestAction implements Action1<TransportMessage> {
		private Queue<GossipTask> queue;

		OnGossipRequestAction(Queue<GossipTask> queue) {
			checkArgument(queue != null);
			this.queue = queue;
		}

		@Override
		public void call(TransportMessage transportMessage) {
			GossipRequest gossipRequest = (GossipRequest) transportMessage.message().data();
			ClusterEndpoint clusterEndpoint = ClusterEndpoint.from(transportMessage.originEndpointId(), transportMessage.originEndpoint());
			for (Gossip gossip : gossipRequest.getGossipList()) {
				queue.offer(new GossipTask(gossip, clusterEndpoint));
			}
		}
	}

	static class GossipDataToGossipWithIncrement implements Function<GossipLocalState, Gossip> {
		@Override
		public Gossip apply(GossipLocalState input) {
			//side effect
			input.incrementSend();
			return input.gossip();
		}
	}

	static class GossipSendPredicate implements Predicate<GossipLocalState> {
		private final ClusterEndpoint endpoint;
		private final int maxCounter;

		GossipSendPredicate(ClusterEndpoint endpoint, int maxCounter) {
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
		private final ClusterEndpoint origin;

		GossipTask(Gossip gossip, ClusterEndpoint origin) {
			this.gossip = gossip;
			this.origin = origin;
		}

		public Gossip getGossip() {
			return gossip;
		}

		public ClusterEndpoint getOrigin() {
			return origin;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			GossipTask that = (GossipTask) o;

			if (origin != null ? !origin.equals(that.origin) : that.origin != null) {
				return false;
			}
			if (gossip != null ? !gossip.equals(that.gossip) : that.gossip != null) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result = gossip != null ? gossip.hashCode() : 0;
			result = 31 * result + (origin != null ? origin.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "GossipTask{" +
					"gossip=" + gossip +
					", clusterEndpoint=" + origin +
					'}';
		}
	}

	class GossipProtocolRunnable implements Runnable {
		@Override
		public void run() {
			try {
				period++;
				Collection<GossipLocalState> gossips = processGossipQueue();
				List<ClusterEndpoint> members = GossipProtocol.this.members;
				int factor = GossipProtocol.this.factor;
				sendGossips(members, gossips, factor);
				sweep(gossips, factor);
			} catch (Exception e) {
				LOGGER.error("Unhandled exception: {}", e, e);
			}
		}
	}
}
