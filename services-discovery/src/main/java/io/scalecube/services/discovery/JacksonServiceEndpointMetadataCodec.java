package io.scalecube.services.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.scalecube.cluster.metadata.MetadataDecoder;
import io.scalecube.cluster.metadata.MetadataEncoder;
import io.scalecube.services.ServiceEndpoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import reactor.core.Exceptions;

public final class JacksonServiceEndpointMetadataCodec implements MetadataDecoder, MetadataEncoder {

  public static final JacksonServiceEndpointMetadataCodec INSTANCE =
      new JacksonServiceEndpointMetadataCodec();

  private static final ObjectMapper objectMapper = newObjectMapper();

  private static ObjectMapper newObjectMapper() {
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

  /**
   * Decodes metadata into {@link ServiceEndpoint}.
   *
   * @param byteBuffer - raw metadata to decode from.
   * @return decoded {@link ServiceEndpoint}
   */
  @SuppressWarnings("unchecked")
  @Override
  public ServiceEndpoint decode(ByteBuffer byteBuffer) {
    try {
      return objectMapper.readValue(
          new ByteBufferBackedInputStream(byteBuffer), ServiceEndpoint.class);
    } catch (IOException ex) {
      throw Exceptions.propagate(ex);
    }
  }

  /**
   * Encodes {@link ServiceEndpoint} into bytes.
   *
   * @param serviceEndpoint - service endpoint to encode
   */
  @Override
  public ByteBuffer encode(Object serviceEndpoint) {
    try {
      return ByteBuffer.wrap(objectMapper.writeValueAsBytes(serviceEndpoint));
    } catch (IOException ex) {
      throw Exceptions.propagate(ex);
    }
  }
}
