import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.DefaultServiceMessageCodec;
import io.scalecube.services.transport.rsocket.JsonServiceMessageCodecImpl;
import io.scalecube.testlib.BaseTest;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;

import org.junit.Test;

public class ServiceMessageDataCodecImplTest extends BaseTest {

  DefaultServiceMessageCodec codec = new DefaultServiceMessageCodec(new JsonServiceMessageCodecImpl());

  @Test
  public void test_encode_decode_ServiceMessage_success() {
    ServiceMessage given = ServiceMessage.builder()
        .header("key1", "hello")
        .header("key2", "world")
        .data(new MyPojo("ronen", 42))
        .build();

    ByteBuf[] bufs = codec.encodeMessage(given);
    Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);

    ServiceMessage message = codec.decodeMessage(payload.sliceData(), payload.sliceMetadata());
    assertEquals("hello", message.header("key1"));
    assertEquals("world", message.header("key2"));
    assertTrue(message.data() instanceof ByteBuf);
  }

  @Test
  public void test_encode_decode_ServiceMessage_data_success() {
    ServiceMessage given = ServiceMessage.builder()
        .header("key1", "hello")
        .header("key2", "world")
        .data(new MyPojo("ronen", 42))
        .build();

    ServiceMessage parsedData = codec.encodeData(given);

    ByteBuf[] bufs = codec.encodeMessage(parsedData);
    Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);

    ServiceMessage message = codec.decodeMessage(payload.sliceData(), payload.sliceMetadata());
    ServiceMessage withData = codec.decodeData(message, MyPojo.class);
    assertEquals("hello", withData.header("key1"));
    assertEquals("world", withData.header("key2"));
    assertTrue(withData.data() instanceof MyPojo);
  }

  @Test
  public void test_encode_decode_ServiceMessage_only_data_success() {
    ServiceMessage given = ServiceMessage.builder()
        .data(new MyPojo("ronen", 42))
        .build();

    ServiceMessage parsedData = codec.encodeData(given);

    ByteBuf[] bufs = codec.encodeMessage(parsedData);
    Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);

    ServiceMessage message = codec.decodeMessage(payload.sliceData(), payload.sliceMetadata());
    ServiceMessage withData = codec.decodeData(message, MyPojo.class);
    assertTrue(withData.data() instanceof MyPojo);
  }

  @Test
  public void test_encode_decode_ServiceMessage_only_header_success() {
    ServiceMessage given = ServiceMessage.builder()
        .header("key1", "hello")
        .header("key2", "world")
        .build();

    ServiceMessage parsedData = codec.encodeData(given);

    ByteBuf[] bufs = codec.encodeMessage(parsedData);
    Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);

    ServiceMessage message = codec.decodeMessage(payload.sliceData(), payload.sliceMetadata());
    ServiceMessage withData = codec.decodeData(message, MyPojo.class);
    assertEquals("hello", withData.header("key1"));
    assertEquals("world", withData.header("key2"));
  }
}
