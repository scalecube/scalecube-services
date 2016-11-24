package io.scalecube.services;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Helper class used to register service with tags as metadata in the scalecube cluster.
 * parsing from service info to json and back.
 */
public class ServiceInfo {

  @JsonIgnore
  private static final ObjectMapper mapper = new ObjectMapper();

  private String serviceName;

  private Tag[] tags;

  public ServiceInfo() {
    // default contractor used for json serialization.
  }

  public ServiceInfo(String serviceName, Tag[] tags) {
    this.serviceName = serviceName;
    this.tags = tags;
  }

  public Tag[] getTags() {
    return tags;
  }

  public String getServiceName() {
    return serviceName;
  }

  private void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  private void setTags(Tag[] tags) {
    this.tags = tags;
  }

  /**
   * Create service Advertisement from a json format.
   * @param value json value of a registration info.
   * @return instance of service Advertisement.
   */
  public static ServiceInfo from(String value) {
    try {
      return mapper.readValue(value, ServiceInfo.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create json format of a service Advertisement.
   * @param value instance value of a Advertisement.
   * @return json value of a registration info.
   */
  public static String toJson(ServiceInfo value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create json format of a service Advertisement.
   * @param serviceName given a service name.
   * @param tags given a service tags.
   * @return  json value of a registration info.
   */
  public static String toJson(String serviceName, Tag[] tags) {
    return toJson(new ServiceInfo(serviceName, tags));
  }
}
