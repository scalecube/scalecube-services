package io.scalecube.services;

import java.util.Collection;
import java.util.Map;

public class ServiceRegistration {

  private String namespace;
  private Map<String, String> tags;
  private Collection<ServiceMethodDefinition> methods;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceRegistration() {}

  public ServiceRegistration(String namespace,
      Map<String, String> tags,
      Collection<ServiceMethodDefinition> methods) {
    this.namespace = namespace;
    this.tags = tags;
    this.methods = methods;
  }

  public String namespace() {
    return namespace;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceMethodDefinition> methods() {
    return methods;
  }

  public ServiceRegistration setTags(Map<String, String> tags) {
    this.tags = tags;
    return this;
  }

  @Override
  public String toString() {
    return "ServiceRegistration{" +
        "namespace='" + namespace + '\'' +
        ", tags=" + tags +
        ", methods=" + methods +
        '}';
  }
}
