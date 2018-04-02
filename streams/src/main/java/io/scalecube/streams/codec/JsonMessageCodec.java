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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class JsonMessageCodec {
  private final ObjectMapper mapper;

  public JsonMessageCodec() {
    this.mapper = initMapper();
  }

  public JsonMessageCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public Object readFrom(InputStream stream, Class clazz) throws IOException {
    return stream.available() <= 0
        ? new Object()
        : mapper.readValue(stream, clazz);
  }

  public StreamMessage decode(StreamMessage message, Class type) throws IOException {
    ByteBufInputStream inputStream = new ByteBufInputStream((ByteBuf) message.data());
    return StreamMessage.from(message).data(readFrom(inputStream, type)).build();
  }

  public void writeTo(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  public StreamMessage encode(StreamMessage message) throws IOException {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    writeTo(new ByteBufOutputStream(buffer), message.data());
    return StreamMessage.from(message).data(buffer).build();
  }

  private ObjectMapper initMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    // Probably not needed
    // objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return objectMapper;
  }
}
