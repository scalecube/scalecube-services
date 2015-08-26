package io.servicefabric.cluster.gossip;

import io.servicefabric.transport.TransportHeaders;
import io.servicefabric.transport.protocol.*;
import io.servicefabric.transport.protocol.ProtostuffMessageDeserializer;
import io.servicefabric.transport.protocol.ProtostuffMessageSerializer;
import io.servicefabric.transport.utils.KVPair;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GossipServiceRequestSchemaTest {

	private static final String testDataQualifier = "servicefabric/testData";

	private TestData testData;

	@Before
	public void init() throws Throwable {
		List<KVPair<String, String>> properties = new ArrayList<>();
		properties.add(new KVPair<>("casino", "123"));

		testData = new TestData();
		testData.setProperties(properties);
	}

	@Test
	public void testProtostuff() throws Exception {
		ProtostuffMessageDeserializer deserializer = new ProtostuffMessageDeserializer();

		ProtostuffMessageSerializer serializer = new ProtostuffMessageSerializer();

		List<Gossip> gossips = getGossips();

		Message message = new Message(new GossipRequest(gossips), TransportHeaders.CORRELATION_ID, "CORR_ID");

		ByteBuf bb = buffer();
		serializer.serialize(message, bb);

		assertTrue(bb.readableBytes() > 0);

		ByteBuf input = copiedBuffer(bb);

		Message deserializedMessage = deserializer.deserialize(input);

		assertNotNull(deserializedMessage);
		Assert.assertEquals(deserializedMessage.data().getClass(), GossipRequest.class);
		Assert.assertEquals("CORR_ID", deserializedMessage.header(TransportHeaders.CORRELATION_ID));

		GossipRequest gossipRequest = (GossipRequest) deserializedMessage.data();
		assertNotNull(gossipRequest);
		assertNotNull(gossipRequest.getGossipList());
		assertNotNull(gossipRequest.getGossipList().get(0));

		Object msg = gossipRequest.getGossipList().get(0).getMessage().data();
		assertNotNull(msg);
		assertTrue(msg.toString(), msg instanceof TestData);
	}

	private List<Gossip> getGossips() {
		Gossip request = new Gossip("idGossip", new Message(testData, TransportHeaders.QUALIFIER, testDataQualifier));
		Gossip request2 = new Gossip("idGossip2", new Message(testData, TransportHeaders.QUALIFIER, testDataQualifier));
		List<Gossip> gossips = new ArrayList<>(2);
		gossips.add(request);
		gossips.add(request2);
		return gossips;
	}

	private static class TestData {

		private List<KVPair<String, String>> properties;

		TestData() {
		}

		public List<KVPair<String, String>> getProperties() {
			return properties;
		}

		public void setProperties(List<KVPair<String, String>> properties) {
			this.properties = properties;
		}
	}

}
