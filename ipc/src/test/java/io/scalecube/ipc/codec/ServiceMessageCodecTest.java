package io.scalecube.ipc.codec;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.scalecube.ipc.ServiceMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

public class ServiceMessageCodecTest {

  @Test
  public void testCodecWithoutAnything() {
    ServiceMessage src = ServiceMessage.builder().build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{}", buf.toString(UTF_8));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    ServiceMessage message = ServiceMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(src, message);
  }

  @Test
  public void testCodecWithOnlyQualifier() {
    ServiceMessage src = ServiceMessage.withQualifier("q").build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\"}", buf.toString(UTF_8));
  }

  @Test
  public void testCodecWithOnlyData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    ServiceMessage src = ServiceMessage.withQualifier((String) null).data(dataBuf).build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}", buf.toString(UTF_8));
  }

  @Test
  public void testCodecWithOnlyQualifierAndData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    ServiceMessage src = ServiceMessage.withQualifier("q").data(dataBuf).build();
    assertEquals("{\"q\":\"q\",\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}",
        ServiceMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithAllFieldsAndNullData() {
    ServiceMessage src = ServiceMessage.withQualifier("q")
        .streamId("stream_id")
        .senderId("sender_id0/sender_id1/sender_id2")
        .data(null).build();
    assertEquals("{\"q\":\"q\"," +
        "\"senderId\":\"sender_id0/sender_id1/sender_id2\"," +
        "\"streamId\":\"stream_id\"}", ServiceMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithEmptyData() {
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("stream_id").data(Unpooled.EMPTY_BUFFER).build();
    assertEquals("{\"q\":\"q\",\"streamId\":\"stream_id\"}", ServiceMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithoutData() {
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("streamId").senderId("senderId").build();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"senderId\":\"senderId\",\"streamId\":\"streamId\"}", buf.toString(UTF_8));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    ServiceMessage message = ServiceMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(null, message.getData());
  }

  @Test
  public void testCodecWithByteBufData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("streamId").data(dataBuf).build();

    int ri = dataBuf.readerIndex();
    ByteBuf buf = ServiceMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"streamId\":\"streamId\",\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}",
        buf.toString(UTF_8));
    assertEquals(ri, dataBuf.readerIndex());

    ServiceMessage message = ServiceMessageCodec.decode(buf.copy());
    assertNotNull(message.getData());
    assertEquals("q", message.getQualifier());
    assertEquals("streamId", message.getStreamId());
    assertEquals(dataBuf.toString(UTF_8), ((ByteBuf) message.getData()).toString(UTF_8));
  }

  @Test
  public void testCodecWithByteBufDataSurrogated() {
    ByteBuf byteSrc =
        copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1,\"something\":\"\ud83d\ude0c\"}", UTF_8);
    ServiceMessage src = ServiceMessage.withQualifier("q").streamId("stream_id").data(byteSrc).build();

    ByteBuf bb = ServiceMessageCodec.encode(src);
    String s = bb.toString(UTF_8);
    assertEquals(s.charAt(98), (char) 0xD83D);
    assertEquals(s.charAt(99), (char) 0xDE0C);
  }

  @Test
  public void testCodecWithByteBufDataNoJsonValidation() {
    ByteBuf buf = copiedBuffer("{\"hello\"w{o{r{l{d", UTF_8);
    int ri = buf.readerIndex();
    ServiceMessage msg = ServiceMessage.withQualifier("q")
        .streamId("stream_id")
        .senderId("sender_id0/sender_id1/sender_id/2")
        .data(buf)
        .build();

    assertEquals("{\"q\":\"q\"," +
        "\"senderId\":\"sender_id0/sender_id1/sender_id/2\"," +
        "\"streamId\":\"stream_id\"," +
        "\"data\":{\"hello\"w{o{r{l{d}", ServiceMessageCodec.encode(msg).toString(UTF_8));
    assertEquals(ri, buf.readerIndex());
  }
}
