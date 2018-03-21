package io.scalecube.streams.codec;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.scalecube.streams.StreamMessage.DATA_NAME;
import static io.scalecube.streams.StreamMessage.QUALIFIER_NAME;
import static io.scalecube.streams.StreamMessage.SUBJECT_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.scalecube.streams.StreamMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class ByteBufCodecTest {

  private static final String QUALIFIER = "io'scalecube'services'transport'\\/Request\\/日本語";

  private static final String SUBJECT = "君が代";

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
      "\"subject\":\"" + SUBJECT + "\"}";

  private StreamMessage.Builder messageBuilder;

  private BiConsumer<String, Object> consumer;

  @Before
  public void setUp() {
    messageBuilder = StreamMessage.builder();
    consumer = (headerName, value) -> {
      switch (headerName) {
        case QUALIFIER_NAME:
          messageBuilder = messageBuilder.qualifier((String) value);
          break;
        case SUBJECT_NAME:
          messageBuilder = messageBuilder.subject((String) value);
          break;
        case DATA_NAME:
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
    ByteBufCodec.decode(copiedBuffer("{\"key\":\"1234\"}", UTF_8), get, match, (headerName, value) -> {
      if ("key".equals(headerName)) {
        messageBuilder = messageBuilder.qualifier((String) value);
      }
    });
    assertEquals("1234", messageBuilder.build().qualifier());
  }

  @Test
  public void testParse() {
    List<String> get = ImmutableList.of("subject", "q");
    List<String> match = ImmutableList.of("data");
    ByteBufCodec.decode(copiedBuffer(source, UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("io'scalecube'services'transport'/Request/日本語", message.qualifier());
    assertEquals(SUBJECT, message.subject());
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
    ByteBufCodec.decode(copiedBuffer(sourceJson, UTF_8), get, match, (headerName, value) -> {
      switch (headerName) {
        case DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
        default:
          // no-op
      }
    });
    StreamMessage message = messageBuilder.build();
    assertEquals("{\"contextId\":\"hack\",\"data\":{[\"yadayada\", 1, 1e-005]},\"q\":\"hack\"}",
        message.data());
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
    ByteBufCodec.decode(copiedBuffer(sourceJson, UTF_8), get, match, (headerName, value) -> {
      switch (headerName) {
        case DATA_NAME:
          messageBuilder = messageBuilder.data(((ByteBuf) value).toString(CharsetUtil.UTF_8));
          break;
        default:
          // no-op
      }
    });
    StreamMessage message = messageBuilder.build();
    assertEquals("{\"contextId\":\"hack\",\"data\":{[\"ya{da}}\",\"ya{da}}\"]},\"q\":\"hack\"}",
        message.data());
  }

  @Test
  public void testParseCrappyHeaders() {
    List<String> get = ImmutableList.of("\"contextId\"", "\"q\"", "\"subject\"");
    List<String> match = ImmutableList.of("data");
    ByteBufCodec.decode(copiedBuffer(source, UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertNull(message.qualifier());
    assertNull(message.subject());
  }

  @Test(expected = IllegalStateException.class)
  public void testParseWithoutDataAndWithUnescapedDoubleQuotesInsideFail() {
    List<String> get = ImmutableList.of("subject");
    List<String> match = ImmutableList.of("data");
    ByteBufCodec.decode(copiedBuffer("{\"subject\":\"" + SUBJECT + "\"\"}", UTF_8), get, match, consumer);
  }

  @Test
  public void testParseWithoutDataAndWithEscapedDoubleQuotes() {
    List<String> get = ImmutableList.of("subject");
    List<String> match = ImmutableList.of("data");
    ByteBufCodec.decode(copiedBuffer("{\"subject\":\"" + SUBJECT + "\\\"\\\"" + "\"}", UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals(SUBJECT + "\"\"", message.subject());
  }

  @Test(expected = IllegalStateException.class)
  public void testParseInvalidColonPlacement() {
    List<String> get = ImmutableList.of("subject", "subject");
    List<String> match = Collections.emptyList();
    ByteBufCodec.decode(copiedBuffer("{\"subject\":\"cool\" :}", UTF_8), get, match, consumer);
    fail("IllegalStateException should be thrown in case of invalid colon placement");
  }

  @Test
  public void testParseInsignificantSymbols() {
    List<String> get = ImmutableList.of("subject");
    List<String> match = Collections.emptyList();
    ByteBufCodec.decode(copiedBuffer("{\n\r\t \"subject\"\n\r\t :\n\r\t \"cool\"\n\r\t }", UTF_8), get, match,
        consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("Whitespaces before and after colon should be ignored", "cool", message.subject());
  }

  @Test
  public void testParseEscapedCharacters() {
    List<String> get = ImmutableList.of("subject");
    List<String> match = Collections.emptyList();
    ByteBufCodec.decode(copiedBuffer("{\"subject\":\"\\\"quoted\\\"\"}", UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "\"quoted\"", message.subject());
  }

  @Test
  public void testSkipExtraFields() {
    List<String> get = ImmutableList.of("q");
    List<String> match = Collections.emptyList();
    ByteBufCodec.decode(copiedBuffer("{\"extra\":\"1234\"," + "\"q\":\"" + 123 + "\"}", UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "123", message.qualifier());
  }

  @Test
  public void testParseEscapeSymbolsInData() {
    List<String> get = ImmutableList.of("q");
    List<String> match = ImmutableList.of("data");
    ByteBufCodec.decode(copiedBuffer("{\"data\":\"\\u1234\"," + "\"q\":\"" + 123 + "\"}", UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "123", message.qualifier());
    assertEquals("Data is not equal", "\"\\u1234\"", ((ByteBuf) message.data()).toString(CharsetUtil.UTF_8));
  }

  @Test
  public void testParseEscapedHexSymbol() {
    List<String> get = ImmutableList.of("q", "subject");
    List<String> match = Collections.emptyList();
    ByteBufCodec.decode(copiedBuffer("{\"q\":\"\\uCEB1\\u0061\\u0063\\u006B\\uaef1\"," +
        "\"subject\":\"\\u0068\\u0061\\u0063\\u006b\"}", UTF_8), get, match, consumer);
    StreamMessage message = messageBuilder.build();
    assertEquals("Parsed string doesn't match source one", "캱ack껱", message.qualifier());
    assertEquals("Parsed string doesn't match source one", "hack", message.subject());
  }

  @Test
  public void testEncodeTwoFlatFieldsMessage() {
    StreamMessage message = StreamMessage.builder().qualifier("/cool/qalifier").subject("subject").build();
    ImmutableList<String> get = ImmutableList.of("q", "subject");
    List<String> match = Collections.emptyList();

    ByteBuf targetBuf = Unpooled.buffer();
    ByteBufCodec.encode(targetBuf, get, match, headerName -> {
      switch (headerName) {
        case QUALIFIER_NAME:
          return message.qualifier();
        case SUBJECT_NAME:
          return message.subject();
        default:
          return null; // skip
      }
    });

    StreamMessage.Builder builder = StreamMessage.builder();
    ByteBufCodec.decode(copiedBuffer(targetBuf), get, match, (headerName, obj) -> {
      switch (headerName) {
        case QUALIFIER_NAME:
          builder.qualifier((String) obj);
          break;
        case SUBJECT_NAME:
          builder.subject((String) obj);
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
    StreamMessage message = StreamMessage.builder().qualifier("q").data(copiedBuffer(dataBuf)).build();

    ByteBuf targetBuf = Unpooled.buffer();
    ImmutableList<String> get = ImmutableList.of("q");
    ImmutableList<String> match = ImmutableList.of("data");
    ByteBufCodec.encode(targetBuf, get, match, headerName -> {
      switch (headerName) {
        case QUALIFIER_NAME:
          return message.qualifier();
        case DATA_NAME:
          return message.data();
        default:
          return null;
      }
    });

    StreamMessage.Builder messageBuilder = StreamMessage.builder();
    ByteBufCodec.decode(copiedBuffer(targetBuf), get, match, (headerName, obj) -> {
      switch (headerName) {
        case QUALIFIER_NAME:
          messageBuilder.qualifier((String) obj);
          break;
        case DATA_NAME:
          messageBuilder.data(obj);
          break;
        default:
          // no-op
      }
    });

    assertEquals(dataBuf.toString(UTF_8), ((ByteBuf) messageBuilder.build().data()).toString(UTF_8));
  }
}
