package io.scalecube.services;

import io.scalecube.streams.StreamMessage;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;

import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class JsonMessageCodecTest {

  public static final String TEST = "test";
  StreamMessageDataCodecImpl codec;

  @Before
  public void setUp() {
    codec = new StreamMessageDataCodecImpl();

  }

  @Test
  public void testEncodeObj() throws Exception {
    // Given
    String payload = "Test";
    StreamMessage msg = StreamMessage.builder().qualifier(TEST).data(payload).build();
    // When:
    StreamMessage encoded = codec.encodeData(msg);
    // Then:
    Assert.assertEquals(msg.qualifier(), encoded.qualifier());
    Assert.assertTrue(ByteBuf.class.isAssignableFrom(encoded.data().getClass()));
    Assert.assertTrue(((ByteBuf) encoded.data()).readableBytes() > 0);
  }

  @Test
  public void testDecodeObj() throws Exception {
    // Given
    String payload = "Test";
    StreamMessage encoded = codec.encodeData(StreamMessage.builder().qualifier(TEST).data(payload).build());
    // When:
    StreamMessage decoded = codec.decodeData(encoded, String.class);
    // Then:
    Assert.assertEquals(decoded.qualifier(), encoded.qualifier());
    Assert.assertTrue(decoded.data() instanceof String);
  }

  @Test
  public void testComplexObjectSerDeser() throws IOException {
    // Given
    TestPayload payload = TestPayload.create();

    // When
    StreamMessage encoded = codec.encodeData(StreamMessage.builder().qualifier(TEST).data(payload).build());
    StreamMessage decoded = codec.decodeData(encoded, TestPayload.class);
    TestPayload deserializedPayload = (TestPayload) decoded.data();

    // Then:
    Assert.assertEquals(payload.num, deserializedPayload.num);
    Assert.assertEquals(payload.lnum, deserializedPayload.lnum);
    Assert.assertEquals(payload.str, deserializedPayload.str);
    Assert.assertEquals(payload.bool, deserializedPayload.bool);
    Assert.assertEquals(payload.num, deserializedPayload.num);
    Assert.assertEquals(payload.arr[0], deserializedPayload.arr[0]);
    Assert.assertEquals(payload.list.get(0), deserializedPayload.list.get(0));
  }

  static class TestPayload {
    int num;
    long lnum;
    String str;
    String[] arr;
    List<String> list;
    Boolean bool;

    public TestPayload() {}

    public TestPayload(int num, long lnum, String str, String[] arr, List<String> list, Boolean bool) {
      this.num = num;
      this.lnum = lnum;
      this.str = str;
      this.arr = arr;
      this.list = list;
      this.bool = bool;
    }

    static TestPayload create() {
      return new TestPayload(
          42, 42l, "test", new String[] {"4"}, Lists.newArrayList("2"), false);
    }
  }
}
