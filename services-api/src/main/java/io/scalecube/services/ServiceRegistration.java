package io.scalecube.services;

import java.util.Collection;
import java.util.Map;

public class ServiceRegistration {

  private final String serviceName;
  private final String contentType;
  private final Map<String, String> tags;
  private final Collection<ServiceMethod> methods;

  public ServiceRegistration(String serviceName,
      String contentType,
      Map<String, String> tags,
      Collection<ServiceMethod> methods) {
    this.serviceName = serviceName;
    this.contentType = contentType;
    this.tags = tags;
    this.methods = methods;
  }

  public String serviceName() {
    return serviceName;
  }

  public String contentType() {
    return contentType;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceMethod> methods() {
    return methods;
  }
}
