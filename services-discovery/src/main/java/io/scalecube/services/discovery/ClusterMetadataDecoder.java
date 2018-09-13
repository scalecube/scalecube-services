package io.scalecube.services.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.scalecube.services.ServiceEndpoint;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;

public class ClusterMetadataDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetadataDecoder.class);

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
   * @param metadata - raw metadata to decode from.
   * @return decoded {@link ServiceEndpoint}. In case of deserialization error returns {@code null}
   */
  public static ServiceEndpoint decodeMetadata(String metadata) {
    try {
      return objectMapper.readValue(metadata, ServiceEndpoint.class);
    } catch (IOException e) {
      LOGGER.error("Can read metadata: " + e, e);
      return null;
    }
  }

  /**
   * Encodes {@link ServiceEndpoint} into raw String.
   *
   * @param serviceEndpoint - service endpoint to encode.
   * @return encoded {@link ServiceEndpoint}. In case of deserialization error throws {@link
   *     IOException}
   */
  public static String encodeMetadata(ServiceEndpoint serviceEndpoint) {
    try {
      return objectMapper.writeValueAsString(serviceEndpoint);
    } catch (IOException e) {
      LOGGER.error("Can write metadata: " + e, e);
      throw Exceptions.propagate(e);
    }
  }
}
