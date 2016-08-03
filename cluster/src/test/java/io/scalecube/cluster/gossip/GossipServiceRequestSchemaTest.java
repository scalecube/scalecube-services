package io.scalecube.cluster.gossip;

import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;
import io.scalecube.transport.MessageDeserializer;
import io.scalecube.transport.MessageSerializer;

import io.netty.buffer.ByteBuf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GossipServiceRequestSchemaTest {

  private static final String testDataQualifier = "scalecube/testData";

  private TestData testData;

  @Before
  public void init() throws Throwable {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "123");

    testData = new TestData();
    testData.setProperties(properties);
  }

  @Test
  public void testProtostuff() throws Exception {

    List<Gossip> gossips = getGossips();

    Message message = Message.withData(new GossipRequest(gossips)).correlationId("CORR_ID").build();

    ByteBuf bb = buffer();
    MessageSerializer serializer = new MessageSerializer();
    serializer.serialize(message, bb);

    assertTrue(bb.readableBytes() > 0);

    ByteBuf input = copiedBuffer(bb);

    MessageDeserializer deserializer = new MessageDeserializer();
    Message deserializedMessage = deserializer.deserialize(input);

    assertNotNull(deserializedMessage);
    Assert.assertEquals(deserializedMessage.data().getClass(), GossipRequest.class);
    Assert.assertEquals("CORR_ID", deserializedMessage.correlationId());

    GossipRequest gossipRequest = deserializedMessage.data();
    assertNotNull(gossipRequest);
    assertNotNull(gossipRequest.getGossipList());
    assertNotNull(gossipRequest.getGossipList().get(0));

    Object msgData = gossipRequest.getGossipList().get(0).getMessage().data();
    assertNotNull(msgData);
    assertTrue(msgData.toString(), msgData instanceof TestData);
    assertEquals(testData.getProperties(), ((TestData) msgData).getProperties());
  }

  private List<Gossip> getGossips() {
    Gossip request = new Gossip("idGossip", Message.withData(testData).qualifier(testDataQualifier).build());
    Gossip request2 = new Gossip("idGossip2", Message.withData(testData).qualifier(testDataQualifier).build());
    List<Gossip> gossips = new ArrayList<>(2);
    gossips.add(request);
    gossips.add(request2);
    return gossips;
  }

  private static class TestData {

    private Map<String, String> properties;

    TestData() {}

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

}
