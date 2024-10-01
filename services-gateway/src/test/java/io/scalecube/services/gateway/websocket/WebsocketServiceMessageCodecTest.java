package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.websocket.GatewayMessages.DATA_FIELD;
import static io.scalecube.services.gateway.websocket.GatewayMessages.INACTIVITY_FIELD;
import static io.scalecube.services.gateway.websocket.GatewayMessages.QUALIFIER_FIELD;
import static io.scalecube.services.gateway.websocket.GatewayMessages.SIGNAL_FIELD;
import static io.scalecube.services.gateway.websocket.GatewayMessages.STREAM_ID_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.services.api.ServiceMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class WebsocketServiceMessageCodecTest {

  private final WebsocketServiceMessageCodec codec = new WebsocketServiceMessageCodec();
  private final ObjectMapper objectMapper = objectMapper();

  @Test
  public void testDecodeNoData() {
    ByteBuf input = toByteBuf(TestInputs.NO_DATA);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertEquals(TestInputs.I, Long.parseLong(message.header(INACTIVITY_FIELD)));
  }

  @Test
  public void testDecodeNullData() {
    Object nullData = "null";
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            nullData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertNull(message.data());
  }

  @Test
  public void testDecodeNumberData() {
    Integer expectedData = 123;
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            expectedData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(
        expectedData, Integer.valueOf(((ByteBuf) message.data()).toString(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDecodeNumberDataFirst() {
    Integer expectedData = 123;
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_D_SIG_SID_Q,
            expectedData,
            TestInputs.SIG,
            TestInputs.SID,
            TestInputs.Q);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(
        expectedData, Integer.valueOf(((ByteBuf) message.data()).toString(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDecodeStringData() {
    String expectedData = "\"test\"";
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            expectedData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(expectedData, ((ByteBuf) message.data()).toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testDecodeBooleanData() {
    Boolean expectedData = Boolean.FALSE;
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            expectedData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(
        expectedData, Boolean.valueOf(((ByteBuf) message.data()).toString(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDecodePojoData() {
    String expectedData =
        "{\"text\":\"someValue\", \"id\":12345, \"empty\":null, \"embedded\":{\"id\":123}}";
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            expectedData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(expectedData, ((ByteBuf) message.data()).toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testDecodeArrayData() {
    String expectedData = "[{\"id\":1}, {\"id\":2}, {\"id\":3}]";
    String stringData =
        String.format(
            TestInputs.STRING_DATA_PATTERN_Q_SIG_SID_D,
            TestInputs.Q,
            TestInputs.SIG,
            TestInputs.SID,
            expectedData);

    ByteBuf input = toByteBuf(stringData);
    System.out.println("Parsing JSON:" + stringData);

    ServiceMessage message = codec.decode(input);

    assertEquals(TestInputs.Q, message.qualifier());
    assertEquals(TestInputs.SIG, Long.parseLong(message.header(SIGNAL_FIELD)));
    assertEquals(TestInputs.SID, Long.parseLong(message.header(STREAM_ID_FIELD)));
    assertTrue(message.data() instanceof ByteBuf);
    assertEquals(expectedData, ((ByteBuf) message.data()).toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testEncodePojoData() throws Exception {
    TestInputs.Entity data = new TestInputs.Entity("test", 123, true);
    ServiceMessage expected =
        ServiceMessage.builder()
            .qualifier(TestInputs.Q)
            .header(STREAM_ID_FIELD, TestInputs.SID)
            .header(SIGNAL_FIELD, TestInputs.SIG)
            .data(toByteBuf(data))
            .build();
    ByteBuf bb = codec.encode(expected);

    ServiceMessage actual = fromByteBuf(bb, TestInputs.Entity.class);

    assertEquals(expected.qualifier(), actual.qualifier());
    assertEquals(expected.header(SIGNAL_FIELD), actual.header(SIGNAL_FIELD));
    assertEquals(expected.header(STREAM_ID_FIELD), actual.header(STREAM_ID_FIELD));
    assertEquals(expected.header(INACTIVITY_FIELD), actual.header(INACTIVITY_FIELD));
    assertEquals(data, actual.data());
  }

  @Test
  public void testEncodeNumberData() throws Exception {
    Integer data = -213;
    ServiceMessage expected =
        ServiceMessage.builder()
            .qualifier(TestInputs.Q)
            .header(STREAM_ID_FIELD, TestInputs.SID)
            .header(SIGNAL_FIELD, TestInputs.SIG)
            .data(toByteBuf(data))
            .build();
    ByteBuf bb = codec.encode(expected);
    ServiceMessage actual = fromByteBuf(bb, Integer.class);

    assertEquals(expected.qualifier(), actual.qualifier());
    assertEquals(expected.header(SIGNAL_FIELD), actual.header(SIGNAL_FIELD));
    assertEquals(expected.header(STREAM_ID_FIELD), actual.header(STREAM_ID_FIELD));
    assertEquals(expected.header(INACTIVITY_FIELD), actual.header(INACTIVITY_FIELD));
    assertEquals(data, actual.data());
  }

  @Test
  public void testEncodeBooleanData() throws Exception {
    Boolean data = true;
    ServiceMessage expected =
        ServiceMessage.builder()
            .qualifier(TestInputs.Q)
            .header(STREAM_ID_FIELD, TestInputs.SID)
            .header(SIGNAL_FIELD, TestInputs.SIG)
            .data(toByteBuf(data))
            .build();
    ByteBuf bb = codec.encode(expected);

    ServiceMessage actual = fromByteBuf(bb, Boolean.class);

    assertEquals(expected.qualifier(), actual.qualifier());
    assertEquals(expected.header(SIGNAL_FIELD), actual.header(SIGNAL_FIELD));
    assertEquals(expected.header(STREAM_ID_FIELD), actual.header(STREAM_ID_FIELD));
    assertEquals(expected.header(INACTIVITY_FIELD), actual.header(INACTIVITY_FIELD));
    assertEquals(data, actual.data());
  }

  private ByteBuf toByteBuf(String data) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    bb.writeBytes(data.getBytes());
    return bb;
  }

  private ByteBuf toByteBuf(Object object) throws IOException {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    objectMapper.writeValue((OutputStream) new ByteBufOutputStream(bb), object);
    return bb;
  }

  private ServiceMessage fromByteBuf(ByteBuf bb, Class<?> dataClass) throws IOException {
    // noinspection unchecked

    Map<String, Object> map =
        objectMapper.readValue((InputStream) new ByteBufInputStream(bb.slice()), HashMap.class);
    ServiceMessage.Builder builder = ServiceMessage.builder();

    Optional.ofNullable(map.get(QUALIFIER_FIELD)).ifPresent(o -> builder.header("q", o));

    Optional.ofNullable(map.get(STREAM_ID_FIELD))
        .ifPresent(o -> builder.header(STREAM_ID_FIELD, o));

    Optional.ofNullable(map.get(SIGNAL_FIELD)).ifPresent(o -> builder.header(SIGNAL_FIELD, o));

    Optional.ofNullable(map.get(INACTIVITY_FIELD))
        .ifPresent(o -> builder.header(INACTIVITY_FIELD, o));

    return builder.data(objectMapper.convertValue(map.get(DATA_FIELD), dataClass)).build();
  }

  private ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
