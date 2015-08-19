package io.servicefabric.cluster;

import static io.servicefabric.cluster.ClusterMemberStatus.SHUTDOWN;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;
import static io.servicefabric.cluster.ClusterMembershipDataUtils.filterData;
import static io.servicefabric.cluster.ClusterMembershipDataUtils.gossipFilterData;
import static io.servicefabric.cluster.ClusterMembershipDataUtils.syncGroupFilter;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.GOSSIP_MEMBERSHIP;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.SYNC;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.SYNC_ACK;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.gossipMembershipFilter;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.syncAckFilter;
import static io.servicefabric.cluster.ClusterMembershipQualifiers.syncFilter;
import static io.servicefabric.transport.TransportEndpoint.tcp;
import io.servicefabric.cluster.fdetector.FailureDetectorEvent;
import io.servicefabric.cluster.fdetector.IFailureDetector;
import io.servicefabric.cluster.gossip.IGossipProtocolSpi;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.TransportTypeRegistry;
import io.servicefabric.transport.protocol.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public final class ClusterMembership implements IClusterMembership {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembership.class);

	private IFailureDetector failureDetector;
	private IGossipProtocolSpi gossipProtocol;
	private int syncTime = 10 * 1000;
	private int syncTimeout = 3 * 1000;
	private int maxSuspectTime = 60 * 1000;
	private int maxShutdownTime = 60 * 1000;
	private String syncGroup = "default";
	private List<TransportEndpoint> wellknownMembers = new ArrayList<>();
	private ITransport transport;
	private final ClusterEndpoint localEndpoint;
	private final Scheduler scheduler;
	private volatile Subscription cmTask;
	private Timer timer;
	private AtomicInteger periodNbr = new AtomicInteger();
	private ClusterMembershipTable membership = new ClusterMembershipTable();
	private Subject<ClusterMember, ClusterMember> subject = new SerializedSubject(PublishSubject.create());
	private Map<String, String> localMetadata = new HashMap<>();

	/** Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK. */
	private Subscriber<TransportMessage> onSyncSubscriber = Subscribers.create(new Action1<TransportMessage>() {
		@Override
		public void call(TransportMessage transportMessage) {
			ClusterMembershipData data = (ClusterMembershipData) transportMessage.message().data();
			List<ClusterMember> updates = membership.merge(data);
			TransportEndpoint endpoint = transportMessage.originEndpoint();
			if (!updates.isEmpty()) {
				LOGGER.debug("Received Sync from {}, updates: {}", endpoint, updates);
				processUpdates(updates, true/*spread gossip*/);
			} else {
				LOGGER.debug("Received Sync from {}, no updates", endpoint);
			}
			String correlationId = transportMessage.message().correlationId();
			ClusterMembershipData syncAckData = new ClusterMembershipData(membership.asList(), syncGroup);
			Message message = new Message(SYNC_ACK, syncAckData, correlationId);
			transport.to(endpoint).send(message);
		}
	});

	/** Merges FD updates and processes them. */
	private Subscriber<FailureDetectorEvent> onFdSubscriber = Subscribers.create(new Action1<FailureDetectorEvent>() {
		@Override
		public void call(FailureDetectorEvent input) {
			List<ClusterMember> updates = membership.merge(input);
			if (!updates.isEmpty()) {
				LOGGER.debug("Received FD event {}, updates: {}", input, updates);
				processUpdates(updates, true/*spread gossip*/);
			}
		}
	});

	/** Merges gossip's {@link ClusterMembershipData} (not spreading gossip further). */
	private Subscriber<ClusterMembershipData> onGossipSubscriber = Subscribers.create(new Action1<ClusterMembershipData>() {
		@Override
		public void call(ClusterMembershipData data) {
			List<ClusterMember> updates = membership.merge(data);
			if (!updates.isEmpty()) {
				LOGGER.debug("Received gossip, updates: {}", updates);
				processUpdates(updates, false/*spread gossip*/);
			}
		}
	});

	ClusterMembership(ClusterEndpoint localEndpoint, Scheduler scheduler) {
		this.localEndpoint = localEndpoint;
		this.scheduler = scheduler;
	}

	public void setFailureDetector(IFailureDetector failureDetector) {
		this.failureDetector = failureDetector;
	}

	public void setGossipProtocol(IGossipProtocolSpi gossipProtocol) {
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

	public void setWellknownMemberList(Collection<TransportEndpoint> wellknownMemberList) {
		Set<TransportEndpoint> set = new HashSet<>(wellknownMemberList);
		set.remove(localEndpoint.endpoint());
		this.wellknownMembers = new ArrayList<>(set);
	}

	public void setSeedMembers(String seedMembers) {
		List<TransportEndpoint> memberList = new ArrayList<>();
		for (String token : new HashSet<>(Splitter.on(',').splitToList(seedMembers))) {
			if (token.length() != 0) {
				try {
					memberList.add(tcp(token));
				} catch (IllegalArgumentException e) {
					LOGGER.warn("Skipped setting wellknown_member, caught: " + e);
				}
			}
		}
		// filter accidental duplicates/locals
		Set<TransportEndpoint> set = new HashSet<>(memberList);
		for (Iterator<TransportEndpoint> i = set.iterator(); i.hasNext();) {
			TransportEndpoint endpoint = i.next();
			String hostAddress = localEndpoint.endpoint().getHostAddress();
			int port = localEndpoint.endpoint().getPort();
			if (endpoint.getPort() == port && endpoint.getHostAddress().equals(hostAddress)) {
				i.remove();
			}
		}
		setWellknownMemberList(set);
	}

	public void setTransport(ITransport transport) {
		this.transport = transport;
	}

	public void setLocalMetadata(Map<String, String> localMetadata) {
		this.localMetadata = localMetadata;
	}

	public List<TransportEndpoint> getWellknownMembers() {
		return new ArrayList<>(wellknownMembers);
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
	public ClusterMember localMember() {
		return membership.get(localEndpoint);
	}

	@Override
	public void start() {
		// Start timer
		timer = new Timer();
		timer.start();

		// Register data types
		TransportTypeRegistry.getInstance().registerType(SYNC, ClusterMembershipData.class);
		TransportTypeRegistry.getInstance().registerType(SYNC_ACK, ClusterMembershipData.class);
		TransportTypeRegistry.getInstance().registerType(GOSSIP_MEMBERSHIP, ClusterMembershipData.class);

		// Register itself initially before SYNC/SYNC_ACK
		List<ClusterMember> updates = membership.merge(new ClusterMember(localEndpoint, TRUSTED, localMetadata));
		processUpdates(updates, false/*spread gossip*/);

		// Listen to SYNC requests from joining/synchronizing members
		transport.listen()
				.filter(syncFilter())
				.filter(syncGroupFilter(syncGroup))
				.map(filterData(localEndpoint))
				.subscribe(onSyncSubscriber);

		// Listen to 'suspected/trusted' events from FailureDetector
		failureDetector.listenStatus().subscribe(onFdSubscriber);

		// Listen to 'membership' message from GossipProtocol
		gossipProtocol.listen()
				.filter(gossipMembershipFilter())
				.map(gossipFilterData(localEndpoint))
				.subscribe(onGossipSubscriber);

		// Conduct 'initialization phase': take wellknown addresses, send SYNC to all and get at least one SYNC_ACK from any of them
		if (!wellknownMembers.isEmpty()) {
			LOGGER.debug("Initialization phase: making first Sync (wellknown_members={})", wellknownMembers);
			doBlockingSync(wellknownMembers);
		}

		// Schedule 'running phase': select randomly single wellknown address, send SYNC and get SYNC_ACK
		if (!wellknownMembers.isEmpty()) {
			cmTask = scheduler.createWorker().schedulePeriodically(new Action0() {
				@Override
				public void call() {
					try {
						List<TransportEndpoint> members = selectRandomMembers(wellknownMembers);
						LOGGER.debug("Running phase: making Sync (selected_members={}))", members);
						doSync(members, scheduler);
					} catch (Exception e) {
						LOGGER.error("Unhandled exception: {}", e, e);
					}
				}
			}, syncTime, syncTime, TimeUnit.MILLISECONDS);
		}
	}

	@Override
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

	private void doBlockingSync(List<TransportEndpoint> members) {
		String period = "" + periodNbr.incrementAndGet();
		sendSync(members, period);

		Future<TransportMessage> future = transport.listen()
				.filter(syncAckFilter(period))
				.filter(syncGroupFilter(syncGroup))
				.map(filterData(localEndpoint))
				.take(1).toBlocking().toFuture();

		TransportMessage message;
		try {
			message = future.get(syncTimeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LOGGER.info("Timeout getting SyncAck from {}", members);
			return;
		}
		onSyncAck(message);
	}

	private void doSync(final List<TransportEndpoint> members, Scheduler scheduler) {
		String period = "" + periodNbr.incrementAndGet();
		sendSync(members, period);
		transport.listen()
				.filter(syncAckFilter(period))
				.filter(syncGroupFilter(syncGroup))
				.map(filterData(localEndpoint))
				.take(1)
				.timeout(syncTimeout, TimeUnit.MILLISECONDS, scheduler)
				.subscribe(Subscribers.create(new Action1<TransportMessage>() {
					@Override
					public void call(TransportMessage transportMessage) {
						onSyncAck(transportMessage);
					}
				}, new Action1<Throwable>() {
					@Override
					public void call(Throwable throwable) {
						LOGGER.info("Timeout getting SyncAck from {}", members);
					}
				}));
	}

	private void sendSync(List<TransportEndpoint> members, String period) {
		ClusterMembershipData syncData = new ClusterMembershipData(membership.asList(), syncGroup);
		Message message = new Message(SYNC, syncData, period/*correlationId*/);
		for (TransportEndpoint endpoint : members) {
			transport.to(endpoint).send(message);
		}
	}

	private void onSyncAck(TransportMessage transportMessage) {
		ClusterMembershipData data = (ClusterMembershipData) transportMessage.message().data();
		TransportEndpoint endpoint = transportMessage.originEndpoint();
		List<ClusterMember> updates = membership.merge(data);
		if (!updates.isEmpty()) {
			LOGGER.debug("Received SyncAck from {}, updates: {}", endpoint, updates);
			processUpdates(updates, true/*spread gossip*/);
		} else {
			LOGGER.debug("Received SyncAck from {}, no updates", endpoint);
		}
	}

	private List<TransportEndpoint> selectRandomMembers(List<TransportEndpoint> members) {
		List<TransportEndpoint> list = new ArrayList<>(members);
		Collections.shuffle(list, ThreadLocalRandom.current());
		return ImmutableList.of(list.get(ThreadLocalRandom.current().nextInt(list.size())));
	}

	/**
	 * Takes {@code updates} and process them in next order:
	 * <ul>
	 *     <li>recalculates 'cluster members' for {@link #gossipProtocol} and {@link #failureDetector} 
	 *     by filtering out {@code REMOVED/SHUTDOWN} members
	 *     </li>
	 *     <li>if {@code spreadGossip} was set {@code true} -- converts {@code updates} to {@link ClusterMembershipData}
	 *     and send it to cluster via {@link #gossipProtocol}</li>
	 *     <li>publishes updates locally (see {@link #listenUpdates()})</li>     
	 *     <li>iterates on {@code updates}, if {@code update} become {@code SUSPECTED} --
	 *     schedules a timer ({@link #maxSuspectTime}) to remove the member (on {@code TRUSTED} -- cancels the timer)</li>
	 *     <li>iterates on {@code updates}, if {@code update} become {@code SHUTDOWN} --
	 *     schedules a timer ({@link #maxShutdownTime}) to remove the member</li>
	 * </ul>
	 *  
	 * @param updates list of updates after merge
	 * @param spreadGossip flag indicating should updates be gossiped to cluster
	 */
	private void processUpdates(List<ClusterMember> updates, boolean spreadGossip) {
		if (updates.isEmpty())
			return;

		// Reset cluster members on FailureDetector and Gossip
		Map<ClusterEndpoint, ClusterMember> members = membership.getTrustedOrSuspected();
		failureDetector.setClusterMembers(members.keySet());
		gossipProtocol.setClusterMembers(members.keySet());

		// Publish updates to cluster
		if (spreadGossip) {
			gossipProtocol.spread(GOSSIP_MEMBERSHIP, new ClusterMembershipData(updates, syncGroup));
		}
		// Publish updates locally
		for (ClusterMember update : updates) {
			subject.onNext(update);
		}

		// Check state transition
		for (final ClusterMember member : updates) {
			LOGGER.debug("Member {} became {}", member.endpoint(), member.status());
			switch (member.status()) {
			case SUSPECTED:
				failureDetector.suspect(member.endpoint());
				timer.schedule(member.endpoint().endpointId(), new Runnable() {
					@Override
					public void run() {
						LOGGER.debug("Time to remove SUSPECTED member={} from membership", member.endpoint());
						processUpdates(membership.remove(member.endpoint()), false/*spread gossip*/);
					}
				}, maxSuspectTime, TimeUnit.MILLISECONDS);
				break;
			case TRUSTED:
				failureDetector.trust(member.endpoint());
				timer.cancel(member.endpoint().endpointId());
				break;
			case SHUTDOWN:
				timer.schedule(new Runnable() {
					@Override
					public void run() {
						LOGGER.debug("Time to remove SHUTDOWN member={} from membership", member.endpoint());
						membership.remove(member.endpoint());
					}
				}, maxShutdownTime, TimeUnit.MILLISECONDS);
				break;
			}
		}
	}

	@Override
	public void leave() {
		ClusterMember r1 = new ClusterMember(localEndpoint, SHUTDOWN, localMetadata);
		gossipProtocol.spread(GOSSIP_MEMBERSHIP, new ClusterMembershipData(ImmutableList.of(r1), syncGroup));
	}
}
