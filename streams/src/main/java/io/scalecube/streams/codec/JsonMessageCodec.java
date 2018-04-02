package io.scalecube.streams.codec;

import io.scalecube.streams.StreamMessage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;

public final class JsonMessageCodec implements MessageDataCodec {
  private final ObjectMapper mapper;

  public JsonMessageCodec() {
    this.mapper = initMapper();
  }

  public JsonMessageCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public StreamMessage decode(StreamMessage message, Type type) throws IOException {
    ByteBufInputStream inputStream = new ByteBufInputStream((ByteBuf) message.data());
    return StreamMessage.from(message).data(readFrom(inputStream, type)).build();
  }

  public StreamMessage encode(StreamMessage message) throws IOException {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    writeTo(new ByteBufOutputStream(buffer), message.data());
    return StreamMessage.from(message).data(buffer).build();
  }

  public Object readFrom(InputStream stream, Type type) throws IOException {
    TypeFactory typeFactory = mapper.reader().getTypeFactory();
    JavaType resolvedType = typeFactory.constructType(type);
    return mapper.readValue(stream, resolvedType);
  }

  public void writeTo(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  private ObjectMapper initMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return objectMapper;
  }
}
