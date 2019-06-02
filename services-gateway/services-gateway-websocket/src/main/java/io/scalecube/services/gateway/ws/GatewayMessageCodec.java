package io.scalecube.services.gateway.ws;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static io.scalecube.services.gateway.ws.GatewayMessage.DATA_FIELD;
import static io.scalecube.services.gateway.ws.GatewayMessage.INACTIVITY_FIELD;
import static io.scalecube.services.gateway.ws.GatewayMessage.RATE_LIMIT_FIELD;
import static io.scalecube.services.gateway.ws.GatewayMessage.SIGNAL_FIELD;
import static io.scalecube.services.gateway.ws.GatewayMessage.STREAM_ID_FIELD;

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
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.gateway.ReferenceCountUtil;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayMessageCodec {
  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayMessageCodec.class);

  private static final ObjectMapper objectMapper = objectMapper();

  private static final MappingJsonFactory jsonFactory = new MappingJsonFactory(objectMapper);

  private final boolean releaseDataOnEncode;

  public GatewayMessageCodec() {
    this(true /*always release by default*/);
  }

  public GatewayMessageCodec(boolean releaseDataOnEncode) {
    this.releaseDataOnEncode = releaseDataOnEncode;
  }

  /**
   * Encode given {@code message} to given {@code byteBuf}.
   *
   * @param message - input message to be encoded.
   * @throws MessageCodecException in case of issues during encoding.
   */
  public ByteBuf encode(GatewayMessage message) throws MessageCodecException {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    try (JsonGenerator generator =
        jsonFactory.createGenerator(
            (OutputStream) new ByteBufOutputStream(byteBuf), JsonEncoding.UTF8)) {
      generator.writeStartObject();

      // headers
      for (Entry<String, String> header : message.headers().entrySet()) {
        String fieldName = header.getKey();
        String value = header.getValue();
        switch (fieldName) {
          case STREAM_ID_FIELD:
          case SIGNAL_FIELD:
          case INACTIVITY_FIELD:
          case RATE_LIMIT_FIELD:
            generator.writeNumberField(fieldName, Long.parseLong(value));
            break;
          default:
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

  /**
   * Decodes {@link GatewayMessage} from given {@code byteBuf}.
   *
   * @param byteBuf - contains raw {@link GatewayMessage} to be decoded.
   * @return Decoded {@link GatewayMessage}.
   * @throws MessageCodecException - in case of issues during deserialization.
   */
  public GatewayMessage decode(ByteBuf byteBuf) throws MessageCodecException {
    try (InputStream stream = new ByteBufInputStream(byteBuf, true)) {
      JsonParser jp = jsonFactory.createParser(stream);
      GatewayMessage.Builder result = GatewayMessage.builder();

      JsonToken current = jp.nextToken();
      if (current != JsonToken.START_OBJECT) {
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

        if (fieldName.equals(DATA_FIELD)) {
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
        result.data(byteBuf.copy((int) dataStart, (int) (dataEnd - dataStart)));
      }
      return result.build();
    } catch (Throwable ex) {
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
