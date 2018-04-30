package io.scalecube.services.transport.rsocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.exceptions.BadRequestException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RSocketJsonPayloadCodec implements ServiceMessageCodec<Payload> {


  @Override
  public String contentType() {
    return "application/json";
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketJsonPayloadCodec.class);

  private final ObjectMapper mapper;

  private TypeReference<Map<String, String>> mapType = new TypeReference<Map<String, String>>() {};

  public RSocketJsonPayloadCodec() {
    this.mapper = initMapper();
  }

  public RSocketJsonPayloadCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Payload encodeMessage(ServiceMessage message) {
    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;

    if (message.data() instanceof ByteBuf) { // has data ?
      dataBuffer = message.data(); // ok so use it as is
    } else if (message.data() != null) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        writeTo(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        LOGGER.error("Message serialization failed: ", ex);
        ReferenceCountUtil.release(dataBuffer);
        throw new BadRequestException("Message serialization failed. " + ex.getMessage());
      }
    }

    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;
    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        writeTo(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(headersBuffer);
      }
    }
    return ByteBufPayload.create(dataBuffer, headersBuffer);
  }

  @Override
  public ServiceMessage decodeMessage(Payload payload) {
    Builder builder = ServiceMessage.builder();
    if (payload.getData().hasRemaining()) {
      builder.data(payload.sliceData());
    }
    if (payload.hasMetadata()) {
      try (ByteBufInputStream inputStream = new ByteBufInputStream(payload.sliceMetadata(), true)) {
        builder.headers(readFrom(inputStream, mapType));
      } catch (Throwable ex) {
        LOGGER.error("Message deserialization failed", ex);
        throw new BadRequestException("Message deserialization failed. " + ex.getMessage());
      }
    }
    return builder.build();
  }

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class type) {
    if (message.data() instanceof ByteBuf) {
      try (ByteBufInputStream inputStream = new ByteBufInputStream(message.data(), true)) {
        return ServiceMessage.from(message).data(readFrom(inputStream, type)).build();
      } catch (Throwable ex) {
        LOGGER.error("Payload deserialization failed: ", ex);
        throw new BadRequestException("Payload deserialization failed. " + ex.getMessage());
      }
    }
    return message;
  }

  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    if (message.data() != null) {
      ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        writeTo(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        LOGGER.error("Payload serialization failed: ", ex);
        ReferenceCountUtil.release(buffer);
        throw new BadRequestException("Payload serialization failed. " + ex.getMessage());
      }
    }
    return message;
  }

  private Object readFrom(InputStream stream, Class<?> type) throws Exception {
    Objects.requireNonNull(type, "ServiceMessageDataCodecImpl.readFrom requires type is not null");
    return mapper.readValue(stream, type);
  }

  private Map<String, String> readFrom(ByteBufInputStream stream, TypeReference<Map<String, String>> type)
      throws IOException {
    Objects.requireNonNull(type, "ServiceMessageDataCodecImpl.readFrom requires type is not null");
    if (stream.available() == 0) {
      return new HashMap<>();
    }
    return mapper.readValue(stream, type);
  }

  private void writeTo(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  private ObjectMapper initMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    objectMapper.registerModule(new JavaTimeModule());

    return objectMapper;
  }
}
