package io.servicefabric.cluster.gossip;

import io.protostuff.runtime.RuntimeSchema;
import io.servicefabric.transport.ITransportTypeRegistry;
import io.servicefabric.transport.TransportTypeRegistry;
import io.servicefabric.transport.protocol.*;
import io.servicefabric.transport.protocol.protostuff.ProtostuffMessageDeserializer;
import io.servicefabric.transport.protocol.protostuff.ProtostuffMessageSerializer;
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

	private static final String gossipQualifier = GossipQualifiers.QUALIFIER;
	private static final String testDataQualifier = "servicefabric/testData";

	private ITransportTypeRegistry typeRegistry;
	private TestData testData;

	@Before
	public void init() throws Throwable {

		typeRegistry = TransportTypeRegistry.getInstance();

		typeRegistry.registerType(gossipQualifier, GossipRequest.class);
		typeRegistry.registerType(testDataQualifier, TestData.class);

		RuntimeSchema.register(Gossip.class, new GossipSchema(typeRegistry));
		List<KVPair<String, String>> properties = new ArrayList<>();
		properties.add(new KVPair<>("casino", "123"));

		testData = new TestData();
		testData.setProperties(properties);
	}

	@Test
	public void testProtostuff() throws Exception {
		ProtostuffMessageDeserializer deserializer = new ProtostuffMessageDeserializer(typeRegistry);

		ProtostuffMessageSerializer serializer = new ProtostuffMessageSerializer();

		List<Gossip> gossips = getGossips();

		Message message = new Message(gossipQualifier, new GossipRequest(gossips), "CORR_ID");

		ByteBuf bb = buffer();
		serializer.serialize(message, bb);

		assertTrue(bb.readableBytes() > 0);

		ByteBuf input = copiedBuffer(bb);

		Message deserialized = deserializer.deserialize(input);

		assertNotNull(deserialized);
		Assert.assertEquals(gossipQualifier, deserialized.qualifier());
		Assert.assertEquals("CORR_ID", deserialized.correlationId());

		GossipRequest gossip = (GossipRequest) deserialized.data();
		assertNotNull(gossip);
		assertNotNull(gossip.getGossipList());
		assertNotNull(gossip.getGossipList().get(0));

		Object msg = gossip.getGossipList().get(0).getData();
		assertNotNull(msg);
		assertTrue(msg.toString(), msg instanceof TestData);
	}

	private List<Gossip> getGossips() {
		Gossip request = new Gossip();
		request.setQualifier(testDataQualifier);
		request.setGossipId("idGossip");
		request.setData(testData);
		Gossip request2 = new Gossip();
		request2.setQualifier(testDataQualifier);
		request2.setGossipId("idGossip2");
		request2.setData(testData);
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
