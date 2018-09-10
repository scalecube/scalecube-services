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
import io.netty.util.ReferenceCountUtil;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.exceptions.MessageCodecException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WebsocketClientCodec implements ClientCodec<ByteBuf> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketClientCodec.class);

  private static final MappingJsonFactory jsonFactory = new MappingJsonFactory(objectMapper());

  private static final String QUALIFIER_FIELD = "q";
  private static final String STREAM_ID_FIELD = "sid";
  private static final String SIGNAL_FIELD = "sig";
  private static final String DATA_FIELD = "d";
  private static final String INACTIVITY_FIELD = "i";

  private final DataCodec dataCodec;

  /**
   * Constructor for codec which encode/decode client message to/from websocket gateway message
   * represented by json and transformed in {@link ByteBuf}.
   *
   * @param dataCodec data message codec.
   */
  public WebsocketClientCodec(DataCodec dataCodec) {
    this.dataCodec = dataCodec;
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

      if (message.qualifier() != null) {
        generator.writeStringField(QUALIFIER_FIELD, message.qualifier());
      }

      String sid = message.header(STREAM_ID_FIELD);
      if (sid != null) {
        generator.writeNumberField(STREAM_ID_FIELD, Long.parseLong(sid));
      }

      String sig = message.header(SIGNAL_FIELD);
      if (sig != null) {
        generator.writeNumberField(SIGNAL_FIELD, Integer.parseInt(sig));
      }

      String inactivity = message.header(INACTIVITY_FIELD);
      if (inactivity != null) {
        generator.writeNumberField(INACTIVITY_FIELD, Integer.parseInt(inactivity));
      }

      Object data = message.data();
      if (data != null) {
        if (data instanceof ByteBuf) {
          ByteBuf dataBin = (ByteBuf) data;
          if (dataBin.readableBytes() > 0) {
            generator.writeFieldName(DATA_FIELD);
            generator.writeRaw(":");
            generator.flush();
            byteBuf.writeBytes(dataBin);
          }
        } else {
          generator.writeObjectField(DATA_FIELD, data);
        }
      }

      generator.writeEndObject();
    } catch (Throwable ex) {
      ReferenceCountUtil.safeRelease(byteBuf);
      LOGGER.error("Failed to encode message: {}", message, ex);
      throw new MessageCodecException("Failed to encode message", ex);
    }
    return byteBuf;
  }

  @Override
  public ClientMessage decode(ByteBuf encodedMessage) {
    try (InputStream stream = new ByteBufInputStream(encodedMessage.slice())) {
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
        switch (fieldName) {
          case QUALIFIER_FIELD:
            result.qualifier(jp.getValueAsString());
            break;
          case STREAM_ID_FIELD:
            result.header(STREAM_ID_FIELD, jp.getValueAsString());
            break;
          case SIGNAL_FIELD:
            result.header(SIGNAL_FIELD, jp.getValueAsString());
            break;
          case INACTIVITY_FIELD:
            result.header(INACTIVITY_FIELD, jp.getValueAsString());
            break;
          case DATA_FIELD:
            dataStart = jp.getTokenLocation().getByteOffset();
            if (current.isScalarValue()) {
              if (!current.isNumeric() && !current.isBoolean()) {
                jp.getValueAsString();
              }
            } else if (current.isStructStart()) {
              jp.skipChildren();
            }
            dataEnd = jp.getCurrentLocation().getByteOffset();
            break;
          default:
            break;
        }
      }
      if (dataEnd > dataStart) {
        result.data(encodedMessage.slice((int) dataStart, (int) (dataEnd - dataStart)));
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
