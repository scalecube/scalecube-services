package io.scalecube.services.transport.jackson;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class JacksonCodec implements DataCodec, HeadersCodec {

  public static final String CONTENT_TYPE = "application/json";

  private final ObjectMapper mapper;

  public JacksonCodec() {
    this(initMapper());
  }

  public JacksonCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String contentType() {
    return CONTENT_TYPE;
  }

  @Override
  public void encode(OutputStream stream, Map<String, String> headers) throws IOException {
    mapper.writeValue(stream, headers);
  }

  @Override
  public void encode(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  @Override
  public Map<String, String> decode(InputStream stream) throws IOException {
    return stream.available() == 0
        ? Collections.emptyMap()
        : mapper.readValue(stream, HashMap.class);
  }

  @Override
  public Object decode(InputStream stream, Class<?> type) throws IOException {
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
