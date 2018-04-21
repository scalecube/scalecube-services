package io.scalecube.services.transport.codec;


import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;

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
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

public final class ServiceMessageDataCodecImpl implements ServiceMessageCodec<Payload> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageDataCodecImpl.class);

  private final ObjectMapper mapper;

  @Override
  public String contentType() {
    return "application/json";
  }
  
  public ServiceMessageDataCodecImpl() {
    this.mapper = initMapper();
  }

  public ServiceMessageDataCodecImpl(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Payload encodeMessage(ServiceMessage message) {
    ByteBuf dataBuffer = ByteBufAllocator.DEFAULT.buffer();
    if (message.data() != null) {
      try {
        writeTo(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
        ReferenceCountUtil.release(dataBuffer);
      }
    }
    ByteBuf headersBuffer = ByteBufAllocator.DEFAULT.buffer();
    if (!message.headers().isEmpty()) {
      try {
        writeTo(new ByteBufOutputStream(headersBuffer), message.data());
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(headersBuffer);
      }
    }
    return DefaultPayload.create(dataBuffer.array(), headersBuffer.array());
  }
  
  @Override
  public ServiceMessage decodeMessage(Payload payload) {
    Builder builder = ServiceMessage.builder();
    
    if (payload.hasMetadata()) {
      ByteBuf headers = payload.sliceMetadata();
      ByteBufInputStream inputStream = new ByteBufInputStream(headers);
      try {
        builder.headers((Map<String, String>) (readFrom(inputStream, Map.class)));
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    
    if (payload.getData().hasRemaining()) {
      try {
        builder.data(payload.sliceData());
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return builder.build();
  }
  
  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class type) {
    if (message.data() != null && message.data() instanceof ByteBuf) {
      ByteBufInputStream inputStream = new ByteBufInputStream(message.data());
      try {
        return ServiceMessage.from(message).data(readFrom(inputStream, type)).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return message;
  }

  
  
  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    if (message.data() != null) {
      try {
        writeTo(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(buffer);
      }
    }
    return message;
  }

  private Object readFrom(InputStream stream, Class<?> type) throws IOException {
    Objects.requireNonNull(type, "ServiceMessageDataCodecImpl.readFrom requires type is not null");
    try {
      return mapper.readValue(stream, type);
    } catch (Throwable ex) {
      throw new RuntimeException("mapper.readValue with type: " + type, ex);
    }
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
