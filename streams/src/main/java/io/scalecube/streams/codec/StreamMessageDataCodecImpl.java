package io.scalecube.streams.codec;

import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.scalecube.streams.StreamMessage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

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

public final class StreamMessageDataCodecImpl implements StreamMessageDataCodec {
  private static Logger LOGGER = LoggerFactory.getLogger(StreamMessageDataCodecImpl.class);

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
        StreamMessage response = StreamMessage.from(message).data(readFrom(inputStream, type)).build();
        return response;
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
    // TypeFactory typeFactory = mapper.reader().getTypeFactory();
    // JavaType resolvedType = typeFactory.constructType(type);

    Object o = mapper.readValue(stream, type);
    return o;
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
