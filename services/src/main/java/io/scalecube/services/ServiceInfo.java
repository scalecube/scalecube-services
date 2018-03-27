package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Map;
import java.util.Set;

/**
 * Helper class used to register service with tags as metadata in the scalecube cluster. parsing from service info to
 * json and back.
 */
public class ServiceInfo {

  private static final ObjectMapper json = ServiceInfo.newMapper();

  private String serviceName;

  private Map<String, String> tags;

  private Set<String> methods;

  private ServiceInfo() {
    // default ctor.
  }

  private static ObjectMapper newMapper() {
    ObjectMapper json = new ObjectMapper();
    json.setVisibility(json.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(Visibility.ANY)
        .withGetterVisibility(Visibility.NONE)
        .withSetterVisibility(Visibility.NONE)
        .withCreatorVisibility(Visibility.NONE));
    json.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return json;
  }

  public ServiceInfo(String serviceName, Set<String> methods, Map<String, String> tags) {
    this.serviceName = serviceName;
    this.tags = tags;
    this.methods = methods;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public String getServiceName() {
    return serviceName;
  }

  /**
   * Gets a service info instance by a given metadata string SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * 
   * @param metadata string as follow : SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * @return initialized service info.
   */
  public static ServiceInfo from(String metadata) {
    requireNonNull(metadata);
    try {
      return json.readValue(metadata, ServiceInfo.class);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * returns a service info as metadata string: SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * 
   * @return initialized service info - SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|..
   */
  public String toMetadata() {
    try {
      return json.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public Set<String> methods() {
    return this.methods;
  }
}
