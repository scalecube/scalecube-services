package io.scalecube.streams.codec;

import io.scalecube.streams.StreamMessage;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public final class StreamMessageDataCodecImpl implements StreamMessageDataCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamMessageDataCodecImpl.class);

  private final ObjectMapper mapper;

  public StreamMessageDataCodecImpl() {
    this.mapper = initMapper();
  }

  public StreamMessageDataCodecImpl(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public StreamMessage decodeData(StreamMessage message, Class type) {
    if (message.data() != null && message.data() instanceof ByteBuf) {
      ByteBufInputStream inputStream = new ByteBufInputStream(message.data());
      try {
        return StreamMessage.from(message).data(readFrom(inputStream, type)).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return message;
  }

  @Override
  public StreamMessage encodeData(StreamMessage message) {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    if (message.data() != null) {
      try {
        writeTo(new ByteBufOutputStream(buffer), message.data());
        return StreamMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
        ReferenceCountUtil.release(buffer);
      }
    }
    return message;
  }

  private Object readFrom(InputStream stream, Class<?> type) throws IOException {
    Objects.requireNonNull(type, "StreamMessageDataCodecImpl.readFrom requires type is not null");
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
