package io.scalecube.gateway.clientsdk.websocket;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.exceptions.MessageCodecException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map.Entry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WebsocketClientCodec implements ClientCodec<ByteBuf> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketClientCodec.class);

  private static final MappingJsonFactory jsonFactory = new MappingJsonFactory(objectMapper());

  private static final String STREAM_ID_FIELD = "sid";
  private static final String SIGNAL_FIELD = "sig";
  private static final String DATA_FIELD = "d";

  private final DataCodec dataCodec;
  private final boolean releaseDataOnEncode;

  /**
   * Constructor for codec which encode/decode client message to/from websocket gateway message
   * represented by json and transformed in {@link ByteBuf}.
   *
   * @param dataCodec data message codec.
   */
  public WebsocketClientCodec(DataCodec dataCodec) {
    this(dataCodec, true /*always release by default*/);
  }

  /**
   * Constructor for codec which encode/decode client message to/from websocket gateway message
   * represented by json and transformed in {@link ByteBuf}.
   *
   * @param dataCodec data message codec.
   * @param releaseDataOnEncode release data on encode flag.
   */
  public WebsocketClientCodec(DataCodec dataCodec, boolean releaseDataOnEncode) {
    this.dataCodec = dataCodec;
    this.releaseDataOnEncode = releaseDataOnEncode; // always release by default
  }

  @Override
  public DataCodec getDataCodec() {
    return dataCodec;
  }

  @Override
  public ByteBuf encode(ClientMessage message) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

    try (JsonGenerator generator =
        jsonFactory.createGenerator(
            (OutputStream) new ByteBufOutputStream(byteBuf), JsonEncoding.UTF8)) {
      generator.writeStartObject();

      // headers
      for (Entry<String, String> header : message.headers().entrySet()) {
        String fieldName = header.getKey();
        String value = header.getValue();
        if (STREAM_ID_FIELD.equals(fieldName) || SIGNAL_FIELD.equals(fieldName)) {
          generator.writeNumberField(fieldName, Long.parseLong(value));
        } else {
          generator.writeStringField(fieldName, value);
        }
      }

      // data
      Object data = message.data();
      if (data != null) {
        if (data instanceof ByteBuf) {
          ByteBuf dataBin = (ByteBuf) data;
          if (dataBin.isReadable()) {
            try {
              generator.writeFieldName(DATA_FIELD);
              generator.writeRaw(":");
              generator.flush();
              byteBuf.writeBytes(dataBin);
            } finally {
              if (releaseDataOnEncode) {
                ReferenceCountUtil.safestRelease(dataBin);
              }
            }
          }
        } else {
          generator.writeObjectField(DATA_FIELD, data);
        }
      }

      generator.writeEndObject();
    } catch (Throwable ex) {
      ReferenceCountUtil.safestRelease(byteBuf);
      Optional.ofNullable(message.data()).ifPresent(ReferenceCountUtil::safestRelease);
      LOGGER.error("Failed to encode message: {}", message, ex);
      throw new MessageCodecException("Failed to encode message", ex);
    }
    return byteBuf;
  }

  @Override
  public ClientMessage decode(ByteBuf encodedMessage) {
    try (InputStream stream = new ByteBufInputStream(encodedMessage.slice(), true)) {
      JsonParser jp = jsonFactory.createParser(stream);
      ClientMessage.Builder result = ClientMessage.builder();

      JsonToken current = jp.nextToken();
      if (current != JsonToken.START_OBJECT) {
        LOGGER.error(
            "Root should be object: {}", encodedMessage.toString(Charset.defaultCharset()));
        throw new MessageCodecException("Root should be object", null);
      }
      long dataStart = 0;
      long dataEnd = 0;
      while ((jp.nextToken()) != JsonToken.END_OBJECT) {
        String fieldName = jp.getCurrentName();
        current = jp.nextToken();
        if (current == VALUE_NULL) {
          continue;
        }

        if (DATA_FIELD.equals(fieldName)) {
          dataStart = jp.getTokenLocation().getByteOffset();
          if (current.isScalarValue()) {
            if (!current.isNumeric() && !current.isBoolean()) {
              jp.getValueAsString();
            }
          } else if (current.isStructStart()) {
            jp.skipChildren();
          }
          dataEnd = jp.getCurrentLocation().getByteOffset();
        } else {
          // headers
          result.header(fieldName, jp.getValueAsString());
        }
      }
      // data
      if (dataEnd > dataStart) {
        result.data(encodedMessage.copy((int) dataStart, (int) (dataEnd - dataStart)));
      }
      return result.build();
    } catch (Throwable ex) {
      LOGGER.error(
          "Failed to decode message: {}", encodedMessage.toString(Charset.defaultCharset()), ex);
      throw new MessageCodecException("Failed to decode message", ex);
    }
  }

  private static ObjectMapper objectMapper() {
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
