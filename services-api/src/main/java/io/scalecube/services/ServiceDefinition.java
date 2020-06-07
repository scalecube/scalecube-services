package io.scalecube.services;

import java.util.Collections;
import java.util.Map;

/** Definition of service - type and tags. */
public class ServiceDefinition {

  private final Class<?> serviceType;
  private final Map<String, String> tags;

  public ServiceDefinition(Class<?> serviceType, Map<String, String> tags) {
    this.serviceType = serviceType;
    this.tags = tags;
  }

  public ServiceDefinition(Class<?> serviceType) {
    this(serviceType, Collections.emptyMap());
  }

  public Class<?> type() {
    return serviceType;
  }

  public Map<String, String> tags() {
    return tags;
  }
}
