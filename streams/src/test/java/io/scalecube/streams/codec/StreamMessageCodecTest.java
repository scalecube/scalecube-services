package io.scalecube.streams.codec;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.scalecube.streams.StreamMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

public class StreamMessageCodecTest {

  @Test
  public void testCodecWithoutAnything() {
    StreamMessage src = StreamMessage.builder().build();
    ByteBuf buf = StreamMessageCodec.encode(src);
    assertEquals("{}", buf.toString(UTF_8));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    StreamMessage message = StreamMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(src, message);
  }

  @Test
  public void testCodecWithOnlyQualifier() {
    StreamMessage src = StreamMessage.builder().qualifier("q").build();
    ByteBuf buf = StreamMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\"}", buf.toString(UTF_8));
  }

  @Test
  public void testCodecWithOnlyData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    StreamMessage src = StreamMessage.builder().qualifier((String) null).data(dataBuf).build();
    ByteBuf buf = StreamMessageCodec.encode(src);
    assertEquals("{\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}", buf.toString(UTF_8));
  }

  @Test
  public void testCodecWithOnlyQualifierAndData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    StreamMessage src = StreamMessage.builder().qualifier("q").data(dataBuf).build();
    assertEquals("{\"q\":\"q\",\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}",
        StreamMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithAllFieldsAndNullData() {
    StreamMessage src = StreamMessage.builder().qualifier("q")
        .subject("id0/id1/id2")
        .data(null).build();
    assertEquals("{\"q\":\"q\",\"subject\":\"id0/id1/id2\"}",
        StreamMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithEmptyData() {
    StreamMessage src = StreamMessage.builder().qualifier("q").subject("subject").data(Unpooled.EMPTY_BUFFER).build();
    assertEquals("{\"q\":\"q\",\"subject\":\"subject\"}", StreamMessageCodec.encode(src).toString(UTF_8));
  }

  @Test
  public void testCodecWithoutData() {
    StreamMessage src = StreamMessage.builder().qualifier("q").subject("subject").build();
    ByteBuf buf = StreamMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"subject\":\"subject\"}", buf.toString(UTF_8));

    ByteBuf buf1 = buf.copy();
    int ri = buf1.readerIndex();
    StreamMessage message = StreamMessageCodec.decode(buf1);
    assertEquals(ri, buf1.readerIndex());
    assertEquals(null, message.data());
  }

  @Test
  public void testCodecWithByteBufData() {
    ByteBuf dataBuf = copiedBuffer("{\"sessiontimerallowed\":1,\"losslimitallowed\":1}", UTF_8);
    StreamMessage src = StreamMessage.builder().qualifier("q").subject("subject").data(dataBuf).build();

    int ri = dataBuf.readerIndex();
    ByteBuf buf = StreamMessageCodec.encode(src);
    assertEquals("{\"q\":\"q\",\"subject\":\"subject\",\"data\":{\"sessiontimerallowed\":1,\"losslimitallowed\":1}}",
        buf.toString(UTF_8));
    assertEquals(ri, dataBuf.readerIndex());

    StreamMessage message = StreamMessageCodec.decode(buf.copy());
    assertNotNull(message.data());
    assertEquals("q", message.qualifier());
    assertEquals("subject", message.subject());
    assertEquals(dataBuf.toString(UTF_8), ((ByteBuf) message.data()).toString(UTF_8));
  }

  @Test
  public void testCodecWithByteBufDataNoJsonValidation() {
    ByteBuf buf = copiedBuffer("{\"hello\"w{o{r{l{d", UTF_8);
    int ri = buf.readerIndex();
    StreamMessage msg = StreamMessage.builder().qualifier("q")
        .subject("id0/id1/id/2")
        .data(buf)
        .build();

    assertEquals("{\"q\":\"q\"," +
        "\"subject\":\"id0/id1/id/2\"," +
        "\"data\":{\"hello\"w{o{r{l{d}", StreamMessageCodec.encode(msg).toString(UTF_8));
    assertEquals(ri, buf.readerIndex());
  }
}
