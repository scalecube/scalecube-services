package io.servicefabric.cluster.fdetector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.servicefabric.cluster.fdetector.FailureDetectorEvent.SUSPECTED;
import static io.servicefabric.cluster.fdetector.FailureDetectorEvent.TRUSTED;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.ACK;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.PING;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.PING_REQ;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.ackFilter;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.pingFilter;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.pingReqFilter;
import static io.servicefabric.cluster.fdetector.FailureDetectorQualifiers.targetFilter;
import static java.lang.Math.min;
import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.TransportTypeRegistry;
import io.servicefabric.transport.protocol.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import com.google.common.collect.Sets;

public final class FailureDetector implements IFailureDetector {
	private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);

	private volatile List<ClusterEndpoint> members = new ArrayList<>();
	private ITransport transport;
	private final ClusterEndpoint localEndpoint;
	private final Scheduler scheduler;
	private Subject<FailureDetectorEvent, FailureDetectorEvent> subject = new SerializedSubject(PublishSubject.create());
	private AtomicInteger periodNbr = new AtomicInteger();
	private Set<ClusterEndpoint> suspectedMembers = Sets.newConcurrentHashSet();
	private ClusterEndpoint pingMember; // for test purpose only
	private List<ClusterEndpoint> randomMembers; // for test purpose only 
	private int pingTime = 2000;
	private int pingTimeout = 1000;
	private int maxEndpointsToSelect = 3;
	private volatile Subscription fdTask;

	/** Listener to PING message and answer with ACK. */
	private Subscriber<TransportMessage> onPingSubscriber = Subscribers.create(new Action1<TransportMessage>() {
		@Override
		public void call(TransportMessage transportMessage) {
			LOGGER.debug("Received Ping from {}", transportMessage.originEndpoint());
			FailureDetectorData data = (FailureDetectorData) transportMessage.message().data();
			String correlationId = transportMessage.message().correlationId();
			send(data.getFrom(), new Message(ACK, data, correlationId));
		}
	});

	/** Listener to PING_REQ message and send PING to requested cluster member. */
	private Subscriber<TransportMessage> onPingReqSubscriber = Subscribers.create(new Action1<TransportMessage>() {
		@Override
		public void call(TransportMessage transportMessage) {
			LOGGER.debug("Received Ping from {}", transportMessage.originEndpoint());
			FailureDetectorData data = (FailureDetectorData) transportMessage.message().data();
			ClusterEndpoint target = data.getTo();
			ClusterEndpoint originalIssuer = data.getFrom();
			String correlationId = transportMessage.message().correlationId();
			FailureDetectorData pingReqData = new FailureDetectorData(localEndpoint, target, originalIssuer);
			send(target, new Message(PING, pingReqData, correlationId));
		}
	});

	/** Listener to ACK with message containing ORIGINAL_ISSUER then convert message to plain ACK and send it to ORIGINAL_ISSUER. */
	private Subscriber<TransportMessage> onAckToOriginalAckSubscriber = Subscribers.create(new Action1<TransportMessage>() {
		@Override
		public void call(TransportMessage transportMessage) {
			FailureDetectorData data = (FailureDetectorData) transportMessage.message().data();
			ClusterEndpoint target = data.getOriginalIssuer();
			String correlationId = transportMessage.message().correlationId();
			FailureDetectorData originalAckData = new FailureDetectorData(target, data.getTo());
			Message message = new Message(ACK, originalAckData, correlationId);
			send(target, message);
		}
	});

	public FailureDetector(ClusterEndpoint localEndpoint, Scheduler scheduler) {
		checkArgument(localEndpoint != null);
		checkArgument(scheduler != null);
		this.localEndpoint = localEndpoint;
		this.scheduler = scheduler;
	}

	public void setPingTime(int pingTime) {
		this.pingTime = pingTime;
	}

	public void setPingTimeout(int pingTimeout) {
		this.pingTimeout = pingTimeout;
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
		LOGGER.debug("Set cluster members: {}", this.members);
	}

	public void setTransport(ITransport transport) {
		this.transport = transport;
	}

	public ITransport getTransport() {
		return transport;
	}

	public ClusterEndpoint getLocalEndpoint() {
		return localEndpoint;
	}

	public List<ClusterEndpoint> getSuspectedMembers() {
		return new ArrayList<>(suspectedMembers);
	}

	/** <b>NOTE:</b> this method is for test purpose only */
	void setPingMember(ClusterEndpoint member) {
		checkNotNull(member);
		checkArgument(member != localEndpoint);
		this.pingMember = member;
	}

	/** <b>NOTE:</b> this method is for test purpose only */
	void setRandomMembers(List<ClusterEndpoint> randomMembers) {
		checkNotNull(randomMembers);
		this.randomMembers = randomMembers;
	}

	@Override
	public void start() {
		// Register data types
		TransportTypeRegistry.getInstance().registerType(PING, FailureDetectorData.class);
		TransportTypeRegistry.getInstance().registerType(PING_REQ, FailureDetectorData.class);
		TransportTypeRegistry.getInstance().registerType(ACK, FailureDetectorData.class);

		transport.listen().filter(pingFilter()).filter(targetFilter(localEndpoint)).subscribe(onPingSubscriber);
		transport.listen().filter(pingReqFilter()).subscribe(onPingReqSubscriber);
		transport.listen().filter(ackFilter()).filter(new Func1<TransportMessage, Boolean>() {
			@Override
			public Boolean call(TransportMessage transportMessage) {
				return ((FailureDetectorData) transportMessage.message().data()).getOriginalIssuer() != null;
			}
		}).subscribe(onAckToOriginalAckSubscriber);

		fdTask = scheduler.createWorker().schedulePeriodically(new Action0() {
			@Override
			public void call() {
				try {
					doPing(members);
				} catch (Exception e) {
					LOGGER.error("Unhandled exception: {}", e, e);
				}
			}
		}, 0, pingTime, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop() {
		if (fdTask != null) {
			fdTask.unsubscribe();
		}
		subject.onCompleted();
		onPingReqSubscriber.unsubscribe();
		onPingReqSubscriber.unsubscribe();
		onAckToOriginalAckSubscriber.unsubscribe();
	}

	@Override
	public Observable<FailureDetectorEvent> listenStatus() {
		return subject;
	}

	@Override
	public void suspect(ClusterEndpoint member) {
		checkNotNull(member);
		suspectedMembers.add(member);
	}

	@Override
	public void trust(ClusterEndpoint member) {
		checkNotNull(member);
		suspectedMembers.remove(member);
	}

	private void doPing(final List<ClusterEndpoint> members) {
		final ClusterEndpoint pingMember = selectPingMember(members);
		if (pingMember == null)
			return;

		final String period = "" + periodNbr.incrementAndGet();
		FailureDetectorData pingData = new FailureDetectorData(localEndpoint, pingMember);
		Message message = new Message(PING, pingData, period/*correlationId*/);
		LOGGER.debug("Send Ping from {} to {}", localEndpoint, pingMember);

		transport.listen()
				.filter(ackFilter(period))
				.filter(new CorrelationFilter(localEndpoint, pingMember))
				.take(1)
				.timeout(pingTimeout, TimeUnit.MILLISECONDS, scheduler)
				.subscribe(Subscribers.create(new Action1<TransportMessage>() {
					@Override
					public void call(TransportMessage transportMessage) {
						LOGGER.debug("Received PingAck from {}", pingMember);
						declareTrusted(pingMember);
					}
				}, new Action1<Throwable>() {
					@Override
					public void call(Throwable throwable) {
						LOGGER.debug("No PingAck from {} within {}ms; about to make PingReq now", pingMember, pingTimeout);
						doPingReq(members, pingMember, period);
					}
				}));

		send(pingMember, message);
	}

	private void doPingReq(List<ClusterEndpoint> members, final ClusterEndpoint targetMember, String period) {
		final int timeout = pingTime - pingTimeout;
		if (timeout <= 0) {
			LOGGER.debug("No PingReq occured, because not time left (pingTime={}, pingTimeout={})", pingTime, pingTimeout);
			declareSuspected(targetMember);
			return;
		}

		final List<ClusterEndpoint> randomMembers = selectRandomMembers(members, maxEndpointsToSelect, targetMember/*exclude*/);
		if (randomMembers.isEmpty()) {
			LOGGER.debug("No PingReq occured, because member selection => []");
			declareSuspected(targetMember);
			return;
		}

		transport.listen()
				.filter(ackFilter(period))
				.filter(new CorrelationFilter(localEndpoint, targetMember))
				.take(1)
				.timeout(timeout, TimeUnit.MILLISECONDS, scheduler)
				.subscribe(Subscribers.create(new Action1<TransportMessage>() {
					@Override
					public void call(TransportMessage transportMessage) {
						LOGGER.debug("PingReq OK (pinger={}, target={})", transportMessage.originEndpoint(), targetMember);
						declareTrusted(targetMember);
					}
				}, new Action1<Throwable>() {
					@Override
					public void call(Throwable throwable) {
						LOGGER.debug("No PingAck on PingReq within {}ms (pingers={}, target={})", randomMembers, targetMember, timeout);
						declareSuspected(targetMember);
					}
				}));

		FailureDetectorData pingReqData = new FailureDetectorData(localEndpoint, targetMember);
		Message message = new Message(PING_REQ, pingReqData, period/*correlationId*/);
		for (ClusterEndpoint randomMember : randomMembers) {
			LOGGER.debug("Send PingReq from {} to {}", localEndpoint, randomMember);
			send(randomMember, message);
		}
	}

	/** Adds given member to {@link #suspectedMembers} and emitting state {@code SUSPECTED}. */
	private void declareSuspected(ClusterEndpoint member) {
		if (suspectedMembers.add(member)) {
			LOGGER.debug("Member {} became SUSPECTED", member);
			subject.onNext(SUSPECTED(member));
		}
	}

	/** Removes given member from {@link #suspectedMembers} and emitting state {@code TRUSTED}. */
	private void declareTrusted(ClusterEndpoint member) {
		if (suspectedMembers.remove(member)) {
			LOGGER.debug("Member {} became TRUSTED", member);
			subject.onNext(TRUSTED(member));
		}
	}

	private void send(ClusterEndpoint endpoint, Message message) {
		transport.to(endpoint.endpoint()).send(message, null);
	}

	private ClusterEndpoint selectPingMember(List<ClusterEndpoint> members) {
		if (pingMember != null)
			return pingMember;
		return members.isEmpty() ? null : selectRandomMembers(members, 1, null).get(0);
	}

	private List<ClusterEndpoint> selectRandomMembers(List<ClusterEndpoint> members, int k, ClusterEndpoint memberToExclude) {
		if (randomMembers != null)
			return randomMembers;

		checkArgument(k > 0, "FailureDetector: k is required!");
		k = min(k, 5);

		List<ClusterEndpoint> list = new ArrayList<>(members);
		list.remove(memberToExclude);

		List<ClusterEndpoint> target = new ArrayList<>(k);
		for (; !list.isEmpty() && k != 0; k--) {
			ClusterEndpoint member = list.get(ThreadLocalRandom.current().nextInt(list.size()));
			target.add(member);
			list.remove(member);
		}
		return target;
	}

	private static class CorrelationFilter implements Func1<TransportMessage, Boolean> {
		final ClusterEndpoint from;
		final ClusterEndpoint target;

		CorrelationFilter(ClusterEndpoint from, ClusterEndpoint target) {
			this.from = from;
			this.target = target;
		}

		@Override
		public Boolean call(TransportMessage transportMessage) {
			FailureDetectorData data = (FailureDetectorData) transportMessage.message().data();
			return from.equals(data.getFrom()) && target.equals(data.getTo()) && data.getOriginalIssuer() == null;
		}
	}
}
