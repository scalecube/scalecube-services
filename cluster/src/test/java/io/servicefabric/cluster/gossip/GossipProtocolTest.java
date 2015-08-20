package io.servicefabric.cluster.gossip;

import static io.servicefabric.transport.TransportEndpoint.from;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SettableFuture;
import io.servicefabric.transport.protocol.Message;
import org.hamcrest.CoreMatchers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.functions.Action1;
import rx.subjects.PublishSubject;

import com.google.common.collect.Lists;
import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.ITransportChannel;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.TransportMessage;

public class GossipProtocolTest {
	private static final int maxGossipSent = 2;
	private static final int gossipTime = 200;
	private static final int maxEndpointsToSelect = 2;

	private Mockery jmockContext;

	private GossipProtocol protocol;
	private PublishSubject subject;
	private ITransport transport;

	private ScheduledExecutorService executorService;
	private List<ClusterEndpoint> members;
	private ITransportChannel transportChannel;

	@Before
	public void init() {
		jmockContext = new Mockery();
		subject = PublishSubject.create();
		transport = jmockContext.mock(ITransport.class);
		transportChannel = jmockContext.mock(ITransportChannel.class);
		executorService = jmockContext.mock(ScheduledExecutorService.class);
		jmockContext.checking(new Expectations() {
			{
				oneOf(transport).listen();
				will(returnValue(subject));
				oneOf(executorService).scheduleWithFixedDelay(with(any(Runnable.class)), with(200l), with(200l),
						with(TimeUnit.MILLISECONDS));
			}
		});
		protocol = new GossipProtocol(ClusterEndpoint.from("local://id@gp"), executorService);
		protocol.setMaxGossipSent(maxGossipSent);
		protocol.setGossipTime(gossipTime);
		protocol.setMaxEndpointsToSelect(maxEndpointsToSelect);
		protocol.setTransport(transport);
		members = Lists.newArrayList();

		members.add(ClusterEndpoint.from("local://id1@1"));
		members.add(ClusterEndpoint.from("local://id2@2"));
		members.add(ClusterEndpoint.from("local://id3@3"));

		protocol.setClusterMembers(this.members);
		protocol.start();
	}

	@After
	public void destroy() {
		jmockContext.checking(new Expectations() {
			{
				oneOf(executorService).shutdownNow();
			}
		});
	}

	@Test
	public void testListenGossips() throws Exception {
		final List<Gossip> res = Lists.newArrayList();
		protocol.listen().subscribe(new Action1<Gossip>() {
			@Override
			public void call(Gossip gossip) {
				res.add(gossip);
			}
		});
		List<Gossip> gossipList = new ArrayList<>();
		gossipList.add(new Gossip("1", "qualifier", " null"));
		gossipList.add(new Gossip("2", "qualifier", " null"));
		gossipList.add(new Gossip("3", "qualifier", " null"));
		GossipRequest gossipRequest = new GossipRequest(gossipList);

		TransportEndpoint endpoint2 = from("local://2");
		TransportEndpoint endpoint1 = from("local://1");

		subject.onNext(new TransportMessage(transportChannel, new Message(GossipQualifiers.QUALIFIER, gossipRequest), endpoint2, "2"));
		subject.onNext(new TransportMessage(transportChannel, new Message("com.pt.openapi.hello/", null), endpoint1, "1"));
		subject.onNext(new TransportMessage(transportChannel, new Message(GossipQualifiers.QUALIFIER, gossipRequest), endpoint1, "1"));
		List<Gossip> second = new ArrayList<>();
		second.add(new Gossip("2", "qualifier", " null"));
		second.add(new Gossip("4", "qualifier", " null"));
		second.add(new Gossip("5", "qualifier", " null"));
		subject.onNext(new TransportMessage(transportChannel, new Message(GossipQualifiers.QUALIFIER, new GossipRequest(second)), endpoint1, "1"));
		Method processGossipQueueMethod = GossipProtocol.class.getDeclaredMethod("processGossipQueue");
		processGossipQueueMethod.setAccessible(true);
		processGossipQueueMethod.invoke(protocol);
		assertEquals(5, res.size());
	}

	@Test
	public void testSendGossips() throws Exception {
		Method sendGossips = GossipProtocol.class.getDeclaredMethod("sendGossips", List.class, Collection.class, Integer.class);
		sendGossips.setAccessible(true);
		List<GossipLocalState> list = new ArrayList<>();

		list.add(GossipLocalState.create(new Gossip("2", "qualifier", " null"), ClusterEndpoint.from("local://id2@2"), 0));
		jmockContext.checking(new Expectations() {
			{
				exactly(maxEndpointsToSelect).of(transport).to(with(any(TransportEndpoint.class)));
				will(returnValue(transportChannel));
				exactly(maxEndpointsToSelect).of(transportChannel).send(with(any(Message.class)));
			}
		});
		sendGossips.invoke(protocol, members, list, 42);
	}
}
