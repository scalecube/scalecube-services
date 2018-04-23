import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.rsocket.RSocketJsonPayloadCodec;
import io.scalecube.testlib.BaseTest;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

import org.junit.Test;

public class ServiceMessageDataCodecImplTest extends BaseTest {

  @Test
  public void test_encode_decode_ServiceMessage_success() {
    
    RSocketJsonPayloadCodec codec = new RSocketJsonPayloadCodec();
    
    Payload payload = codec.encodeMessage(ServiceMessage.builder()
          .header("key1", "hello")
          .header("key2", "world")
          .data(new MyPojo("ronen",42))
          .build());
    
    ServiceMessage message = codec.decodeMessage(payload);
    assertEquals("hello", message.header("key1"));
    assertEquals("world", message.header("key2"));
    assertTrue(message.data() instanceof ByteBuf);
    
  }
  
  @Test
  public void test_encode_decode_ServiceMessage_data_success() {
    
    RSocketJsonPayloadCodec codec = new RSocketJsonPayloadCodec();
    ServiceMessage given = ServiceMessage.builder()
        .header("key1", "hello")
        .header("key2", "world")
        .data(new MyPojo("ronen",42))
        .build();
    
    ServiceMessage parsedData = codec.encodeData(given);
    
    Payload payload = codec.encodeMessage(parsedData);
    
    ServiceMessage message = codec.decodeMessage(payload);
    ServiceMessage withData = codec.decodeData(message,MyPojo.class);
    assertEquals("hello", withData.header("key1"));
    assertEquals("world", withData.header("key2"));
    assertTrue(withData.data() instanceof MyPojo);
    
  }
  
  @Test
  public void test_encode_decode_ServiceMessage_only_data_success() {
    
    RSocketJsonPayloadCodec codec = new RSocketJsonPayloadCodec();
    ServiceMessage given = ServiceMessage.builder()
        .data(new MyPojo("ronen",42))
        .build();
    
    ServiceMessage parsedData = codec.encodeData(given);
    
    Payload payload = codec.encodeMessage(parsedData);
    
    ServiceMessage message = codec.decodeMessage(payload);
    ServiceMessage withData = codec.decodeData(message,MyPojo.class);
    assertTrue(withData.data() instanceof MyPojo);
    
  }
  
  @Test
  public void test_encode_decode_ServiceMessage_only_header_success() {
    
    RSocketJsonPayloadCodec codec = new RSocketJsonPayloadCodec();
    ServiceMessage given = ServiceMessage.builder()
        .header("key1", "hello")
        .header("key2", "world")
        .build();
    
    ServiceMessage parsedData = codec.encodeData(given);
    
    Payload payload = codec.encodeMessage(parsedData);
    
    ServiceMessage message = codec.decodeMessage(payload);
    ServiceMessage withData = codec.decodeData(message,MyPojo.class);
    assertEquals("hello", withData.header("key1"));
    assertEquals("world", withData.header("key2"));
    
    
  }
}