package io.scalecube.streams.codec;

import io.scalecube.streams.StreamMessage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public final class JsonMessageCodec {
  private final ObjectMapper mapper;

  public JsonMessageCodec(TypeResolver typeResolver) {
    this.mapper = initMapper();
    // register custom serializer/deserializer for message
    SimpleModule module = new SimpleModule();
    module.addSerializer(StreamMessage.class, new JsonMessageSerializer(mapper));
    module.addDeserializer(StreamMessage.class, new JsonMessageDeserializer(typeResolver, mapper));
    mapper.registerModule(module);
  }


  public Object readFrom(InputStream stream, Type type) throws IOException {
    if (stream.available() <= 0) {
      return new Object();
    }
    TypeFactory typeFactory = mapper.reader().getTypeFactory();
    JavaType resolvedType = typeFactory.constructType(type);
    return mapper.readValue(stream, resolvedType);
  }

  public void writeTo(OutputStream stream, Object value) throws IOException {
    mapper.writeValue(stream, value);
  }

  private static class JsonMessageDeserializer extends StdDeserializer<StreamMessage> {
    private final TypeResolver typeResolver;
    private final ObjectMapper mapper;

    public JsonMessageDeserializer(TypeResolver typeResolver, ObjectMapper mapper) {
      super(StreamMessage.class);
      this.typeResolver = typeResolver;
      this.mapper = mapper;
    }

    @Override
    public StreamMessage deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      StreamMessage.Builder messageBuilder = StreamMessage.builder();
      String qualifier = null;

      // generic message headers
      if (jsonNode.has(StreamMessage.QUALIFIER_NAME) && jsonNode.get(StreamMessage.QUALIFIER_NAME).isTextual()) {
        qualifier = jsonNode.get(StreamMessage.QUALIFIER_NAME).asText();
        messageBuilder.qualifier(qualifier);
      }
      if (jsonNode.has(StreamMessage.SUBJECT_NAME) && jsonNode.get(StreamMessage.SUBJECT_NAME).isTextual()) {
        messageBuilder.subject(jsonNode.get(StreamMessage.SUBJECT_NAME).asText());
      }

      // data
      if (jsonNode.has(StreamMessage.DATA_NAME)) {
        String content = jsonNode.get(StreamMessage.DATA_NAME).toString();
        Class<?> resolvedClass = typeResolver.resolveType(qualifier);
        messageBuilder.data(resolvedClass != null ? mapper.readValue(content, resolvedClass) : content);
      }
      return messageBuilder.build();
    }
  }

  private static class JsonMessageSerializer extends StdSerializer<StreamMessage> {
    private final ObjectMapper mapper;

    public JsonMessageSerializer(ObjectMapper mapper) {
      super(StreamMessage.class);
      this.mapper = mapper;
    }

    @Override
    public void serialize(StreamMessage msg, JsonGenerator generator, SerializerProvider provider) throws IOException {
      generator.writeStartObject();

      // generic StreamMessage headers
      if (msg.qualifier() != null) {
        generator.writeStringField(StreamMessage.QUALIFIER_NAME, msg.qualifier());
      }
      if (msg.containsSubject()) {
        generator.writeStringField(StreamMessage.SUBJECT_NAME, msg.subject());
      }

      // data
      Object data = msg.data();
      if (data != null) {
        if (data instanceof byte[]) {
          generator.writeFieldName(StreamMessage.DATA_NAME);
          generator.writeRawValue(new String((byte[]) data, StandardCharsets.UTF_8));
        } else if (data instanceof String) {
          generator.writeFieldName(StreamMessage.DATA_NAME);
          generator.writeRawValue((String) data);
        } else {
          generator.writeFieldName(StreamMessage.DATA_NAME);
          mapper.writeValue(generator, data);
        }
      }
      generator.writeEndObject();
      generator.close();
    }
  }

  private ObjectMapper initMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return objectMapper;
  }
}
