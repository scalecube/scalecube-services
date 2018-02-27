package io.scalecube.ipc.codec;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.scalecube.ipc.ServiceMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class JsonCodecTest {

  private static final String QUALIFIER = "io'scalecube'services'transport'\\/Request\\/日本語";

  private static final String STREAM_ID = "君が代";

  private final String source = "{" +
      "\"data\":{" +
      "   \"favoriteSong\":\"Нино\'С\'Кем\'Ты пьёшь вино\"," +
      "   \"q\":\"hack\"," +
      "   \"contextId\":\"hack\"," +
      "   \"data\":{[\"yadayada\",\"yadayada\",\"yadayada\",\"yadayada\"]}," +
      "   \"language\":\"en\"," +
      "   \"q\":\"hack\"," +
      "   \"contextId\":\"hack\"," +
      "   \"_type\":\"io.scalecube.services.transport.Request\"" +
      "}," +
      "\"q\":\"" + QUALIFIER + "\"," +
      "\"streamId\":\"" + STREAM_ID + "\"}";

  private ServiceMessage.Builder messageBuilder;

  private BiConsumer<String, Object> consumer;

  @Before
  public void setUp() {
    messageBuilder = ServiceMessage.builder();
    consumer = (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.QUALIFIER_NAME:
          messageBuilder = messageBuilder.qualifier((String) value);
          break;
        case ServiceMessage.STREAM_ID_NAME:
          messageBuilder = messageBuilder.streamId((String) value);
          break;
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(value);
          break;
        default:
          // no-op
      }
    };
  }

  @Test
  public void testParseSimple() {
    List<String> get = ImmutableList.of("key");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\"key\":\"1234\"}", UTF_8), get, match, (headerName, value) -> {
      if ("key".equals(headerName)) {
        messageBuilder = messageBuilder.qualifier((String) value);
      }
    });
    assertEquals("1234", messageBuilder.build().getQualifier());
  }

  @Test
  public void testParse() {
    List<String> get = ImmutableList.of("streamId", "q");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer(source, UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("io'scalecube'services'transport'/Request/日本語", message.getQualifier());
    assertEquals(STREAM_ID, message.getStreamId());
  }

  @Test
  public void testParseAndCheckData() {
    String sourceJson = "{" +
        "\"data\":{" +
        "\"contextId\":\"hack\"," +
        "\"data\":{[\"yadayada\", 1, 1e-005]}," +
        "\"q\":\"hack\"" +
        "}";

    List<String> get = ImmutableList.of("nothing");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer(sourceJson, UTF_8), get, match, (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
        default:
          // no-op
      }
    });
    ServiceMessage message = messageBuilder.build();
    assertEquals("{\"contextId\":\"hack\",\"data\":{[\"yadayada\", 1, 1e-005]},\"q\":\"hack\"}",
        message.getData());
  }

  @Test
  public void testParseAndCheckDataTrickyCurlyBrace() {
    String sourceJson = "{" +
        "\"data\":{" +
        "\"contextId\":\"hack\"," +
        "\"data\":{[\"ya{da}}\",\"ya{da}}\"]}," +
        "\"q\":\"hack\"" +
        "}";

    List<String> get = ImmutableList.of("nothing");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer(sourceJson, UTF_8), get, match, (headerName, value) -> {
      switch (headerName) {
        case ServiceMessage.DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
        default:
          // no-op
      }
    });
    ServiceMessage message = messageBuilder.build();
    assertEquals("{\"contextId\":\"hack\",\"data\":{[\"ya{da}}\",\"ya{da}}\"]},\"q\":\"hack\"}",
        message.getData());
  }

  @Test
  public void testParseCrappyHeaders() {
    List<String> get = ImmutableList.of("\"contextId\"", "\"q\"", "\"streamId\"");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer(source, UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertNull(message.getQualifier());
    assertNull(message.getStreamId());
  }

  @Test(expected = IllegalStateException.class)
  public void testParseWithoutDataAndWithUnescapedDoubleQuotesInsideFail() {
    List<String> get = ImmutableList.of("streamId");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer("{\"streamId\":\"" + STREAM_ID + "\"\"}", UTF_8), get, match, consumer);
  }

  @Test
  public void testParseWithoutDataAndWithEscapedDoubleQuotes() {
    List<String> get = ImmutableList.of("streamId");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer("{\"streamId\":\"" + STREAM_ID + "\\\"\\\"" + "\"}", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals(STREAM_ID + "\"\"", message.getStreamId());
  }

  @Test(expected = IllegalStateException.class)
  public void testParseInvalidColonPlacement() {
    List<String> get = ImmutableList.of("streamId", "streamId");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\"streamId\":\"cool\" :}", UTF_8), get, match, consumer);
    fail("IllegalStateException should be thrown in case of invalid colon placement");
  }

  @Test
  public void testParseInsignificantSymbols() {
    List<String> get = ImmutableList.of("streamId");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\n\r\t \"streamId\"\n\r\t :\n\r\t \"cool\"\n\r\t }", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("Whitespaces before and after colon should be ignored", "cool", message.getStreamId());
  }

  @Test
  public void testParseEscapedCharacters() {
    List<String> get = ImmutableList.of("streamId");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\"streamId\":\"\\\"quoted\\\"\"}", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "\"quoted\"", message.getStreamId());
  }

  @Test
  public void testSkipExtraFields() {
    List<String> get = ImmutableList.of("q");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\"extra\":\"1234\"," + "\"q\":\"" + 123 + "\"}", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "123", message.getQualifier());
  }

  @Test
  public void testParseEscapeSymbolsInData() {
    List<String> get = ImmutableList.of("q");
    List<String> match = ImmutableList.of("data");
    JsonCodec.decode(copiedBuffer("{\"data\":\"\\u1234\"," + "\"q\":\"" + 123 + "\"}", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "123", message.getQualifier());
    assertEquals("Data is not equal", "\"\\u1234\"", ((ByteBuf) message.getData()).toString(CharsetUtil.UTF_8));
  }

  @Test
  public void testParseEscapedHexSymbol() {
    List<String> get = ImmutableList.of("q", "streamId");
    List<String> match = Collections.emptyList();
    JsonCodec.decode(copiedBuffer("{\"q\":\"\\uCEB1\\u0061\\u0063\\u006B\\uaef1\"," +
        "\"streamId\":\"\\u0068\\u0061\\u0063\\u006b\"}", UTF_8), get, match, consumer);
    ServiceMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "캱ack껱", message.getQualifier());
    assertEquals("Parsed string doesn't match source one", "hack", message.getStreamId());
  }

  @Test
  public void testEncodeTwoFlatFieldsMessage() {
    ServiceMessage message = ServiceMessage.withQualifier("/cool/qalifier").streamId("stream_id").build();
    ImmutableList<String> get = ImmutableList.of("q", "streamId", "senderId");
    List<String> match = Collections.emptyList();

    ByteBuf targetBuf = Unpooled.buffer();
    JsonCodec.encode(targetBuf, get, match, headerName -> {
      switch (headerName) {
        case "q":
          return message.getQualifier();
        case "streamId":
          return message.getStreamId();
        default:
          return null; // skip senderId
      }
    });

    ServiceMessage.Builder builder = ServiceMessage.builder();
    JsonCodec.decode(copiedBuffer(targetBuf), get, match, (headerName, obj) -> {
      switch (headerName) {
        case "q":
          builder.qualifier((String) obj);
          break;
        case "streamId":
          builder.streamId((String) obj);
          break;
        default:
          // no-op
      }
    });

    assertEquals(message, builder.build());
  }

  @Test
  public void testEncodeWithDataFieldMessage() {
    ByteBuf dataBuf = copiedBuffer("{\"greeting\":\"yadayada\"}", UTF_8);
    ServiceMessage message = ServiceMessage.withQualifier("q").data(copiedBuffer(dataBuf)).build();

    ByteBuf targetBuf = Unpooled.buffer();
    ImmutableList<String> get = ImmutableList.of("q");
    ImmutableList<String> match = ImmutableList.of("data");
    JsonCodec.encode(targetBuf, get, match, headerName -> {
      switch (headerName) {
        case "q":
          return message.getQualifier();
        case "data":
          return message.getData();
        default:
          return null;
      }
    });

    ServiceMessage.Builder messageBuilder = ServiceMessage.builder();
    JsonCodec.decode(copiedBuffer(targetBuf), get, match, (headerName, obj) -> {
      switch (headerName) {
        case "q":
          messageBuilder.qualifier((String) obj);
        case "data":
          messageBuilder.data(obj);
      }
    });

    assertEquals(dataBuf.toString(UTF_8), ((ByteBuf) messageBuilder.build().getData()).toString(UTF_8));
  }
}
