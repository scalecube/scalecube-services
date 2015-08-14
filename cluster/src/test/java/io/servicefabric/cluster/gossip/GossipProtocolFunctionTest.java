package io.servicefabric.cluster.gossip;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import io.servicefabric.transport.protocol.Message;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.ITransportChannel;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.TransportMessage;

public class GossipProtocolFunctionTest {

	private ClusterEndpoint remote;
	private ClusterEndpoint local;
	private ITransportChannel transportChannel;

	@Before
	public void setup() {
		remote = ClusterEndpoint.from("local://id1@peer/remote");
		local = ClusterEndpoint.from("local://id2@peer/local");
		Mockery jmockContext = new Mockery();
		transportChannel = jmockContext.mock(ITransportChannel.class);
	}

	@Test
	public void testGossipMessageFilter() {
		GossipProtocol.GossipMessageFilter filter = new GossipProtocol.GossipMessageFilter();
		Message message = new Message(GossipQualifiers.QUALIFIER, null);
		TransportEndpoint endpoint = TransportEndpoint.from("local://1");
		assertTrue(filter.call(new TransportMessage(transportChannel, message, endpoint, "1")));
		assertFalse(filter.call(new TransportMessage(transportChannel, new Message("com.pt.openapi.hello/"), endpoint, "2")));
	}

	@Test
	public void testOnGossipAction() {
		Queue<GossipProtocol.GossipTask> gossipQueue = new LinkedList<>();
		GossipProtocol.OnGossipRequestAction action = new GossipProtocol.OnGossipRequestAction(gossipQueue);
		TransportEndpoint endpoint = TransportEndpoint.from("local://1");
		List<Gossip> gossips = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			Gossip gossip = new Gossip("" + i, "123", null);
			gossips.add(gossip);
		}
		Message message = new Message(GossipQualifiers.QUALIFIER, new GossipRequest(gossips));

		action.call(new TransportMessage(transportChannel, message, endpoint, "1"));
		assertTrue(gossipQueue.size() == 20);
	}

	@Test
	public void testGossipDataToGossip() {
		Gossip gossip = new Gossip("1", "123", "123");
		GossipLocalState info = GossipLocalState.create(gossip, null, 0);
		GossipProtocol.GossipDataToGossipWithIncrement transform = new GossipProtocol.GossipDataToGossipWithIncrement();
		Gossip apply = transform.apply(info);
		Assert.assertEquals(gossip, apply);
		Assert.assertEquals(1, info.getSent());
	}

	@Test
	public void testGossipSendPredicate() {
		GossipProtocol.GossipSendPredicate predicate = new GossipProtocol.GossipSendPredicate(remote, 3);
		GossipLocalState info = GossipLocalState.create(new Gossip("1", null, null), local, 0);
		assertTrue(predicate.apply(info));
		info.addMember(remote);
		assertFalse(predicate.apply(info));
		GossipLocalState anotherInfo = GossipLocalState.create(new Gossip("2", null, null), local, 0);
		anotherInfo.incrementSend();
		anotherInfo.incrementSend();
		anotherInfo.incrementSend();
		assertFalse(predicate.apply(anotherInfo));

	}

	@Test
	public void testGossipSweepPredicate() {
		GossipProtocol.GossipSweepPredicate predicate = new GossipProtocol.GossipSweepPredicate(100, 10);
		GossipLocalState info = GossipLocalState.create(new Gossip("1", null, null), local, 50);
		assertTrue(predicate.apply(info));
		assertFalse(predicate.apply(GossipLocalState.create(new Gossip("1", null, null), local, 95)));
		assertFalse(predicate.apply(GossipLocalState.create(new Gossip("1", null, null), local, 90)));
	}
}
