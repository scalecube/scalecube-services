package io.scalecube.services;

import java.util.Collection;
import java.util.Map;

public class ServiceRegistration {

  private String namespace;
  private String contentType;
  private Map<String, String> tags;
  private Collection<ServiceMethodDefinition> methods;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceRegistration() {}

  public ServiceRegistration(String namespace,
      String contentType,
      Map<String, String> tags,
      Collection<ServiceMethodDefinition> methods) {
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

  public Collection<ServiceMethodDefinition> methods() {
    return methods;
  }

  public ServiceRegistration setContentType(String contentType) {
    this.contentType = contentType;
    return this;
  }

  public ServiceRegistration setTags(Map<String, String> tags) {
    this.tags = tags;
    return this;
  }

  @Override
  public String toString() {
    return "ServiceRegistration{" +
            "namespace='" + namespace + '\'' +
            ", contentType='" + contentType + '\'' +
            ", tags=" + tags +
            ", methods=" + methods +
            '}';
  }
}
