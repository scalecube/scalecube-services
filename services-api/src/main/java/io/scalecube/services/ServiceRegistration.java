package io.scalecube.services;

import java.util.Collection;
import java.util.Map;

public class ServiceRegistration {

  private String namespace;
  private String contentType;
  private Map<String, String> tags;
  private Collection<ServiceMethod> methods;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceRegistration() {}

  public ServiceRegistration(String namespace,
      String contentType,
      Map<String, String> tags,
      Collection<ServiceMethod> methods) {
    this.namespace = namespace;
    this.contentType = contentType;
    this.tags = tags;
    this.methods = methods;
  }

  public String namespace() {
    return namespace;
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
