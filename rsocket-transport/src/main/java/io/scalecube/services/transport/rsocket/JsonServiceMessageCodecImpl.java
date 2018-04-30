package io.scalecube.services.transport.rsocket;

import io.scalecube.services.codecs.api.ServiceMessageCodecInterface;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class JsonServiceMessageCodecImpl implements ServiceMessageCodecInterface {

  private final TypeReference<Map<String, String>> mapType = new TypeReference<Map<String, String>>() {};

  private final ObjectMapper mapper;

  public JsonServiceMessageCodecImpl() {
    this(initMapper());
  }

  public JsonServiceMessageCodecImpl(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String contentType() {
    return "application/json";
  }

  @Override
  public void writeHeaders(OutputStream stream, Map<String, String> headers) throws IOException {
    mapper.writeValue(stream, headers);
  }

  @Override
  public Map<String, String> readHeaders(InputStream stream) throws IOException {
    return stream.available() == 0 ? Collections.emptyMap() : mapper.readValue(stream, mapType);
  }

  @Override
  public void writeBody(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  @Override
  public Object readBody(InputStream stream, Class<?> type) throws IOException {
    Objects.requireNonNull(type, "ServiceMessageDataCodecImpl.readFrom requires type is not null");
    return mapper.readValue(stream, type);
  }

  private static ObjectMapper initMapper() {
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
