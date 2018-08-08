package io.scalecube.services.discovery;

import io.scalecube.services.ServiceEndpoint;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Exceptions;

import java.io.IOException;

public class ClusterMetadataDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetadataDecoder.class);

  private static final ObjectMapper objectMapper = newObjectMapper();

  private static ObjectMapper newObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return objectMapper;
  }

  public static ServiceEndpoint decodeMetadata(String metadata) {
    try {
      return objectMapper.readValue(metadata, ServiceEndpoint.class);
    } catch (IOException e) {
      LOGGER.error("Can read metadata: " + e, e);
      return null;
    }
  }

  public static String encodeMetadata(ServiceEndpoint serviceEndpoint) {
    try {
      return objectMapper.writeValueAsString(serviceEndpoint);
    } catch (IOException e) {
      LOGGER.error("Can write metadata: " + e, e);
      throw Exceptions.propagate(e);
    }
  }
}
