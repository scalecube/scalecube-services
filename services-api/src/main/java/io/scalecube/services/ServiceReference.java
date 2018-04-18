package io.scalecube.services;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ServiceReference {

  private String endpointId;
  private String host;
  private int port;
  private String namespace;
  private String contentType;
  private Map<String, String> tags;
  private Collection<ServiceMethod> methods;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceReference() {}

  public ServiceReference(ServiceRegistration serviceRegistration, ServiceEndpoint serviceEndpoint) {
    this.endpointId = serviceEndpoint.endpointId();
    this.host = serviceEndpoint.host();
    this.port = serviceEndpoint.port();
    this.namespace = serviceRegistration.namespace();
    this.contentType = mergeContentType(serviceRegistration, serviceEndpoint);
    this.tags = mergeTags(serviceRegistration, serviceEndpoint);
    this.methods = serviceRegistration.methods();
  }

  public String endpointId() {
    return endpointId;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
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

  private Map<String, String> mergeTags(ServiceRegistration serviceRegistration, ServiceEndpoint serviceEndpoint) {
    Map<String, String> tags = new HashMap<>();
    tags.putAll(serviceEndpoint.tags());
    tags.putAll(serviceRegistration.tags());
    return tags;
  }

  private String mergeContentType(ServiceRegistration serviceRegistration, ServiceEndpoint serviceEndpoint) {
    return Optional.ofNullable(serviceRegistration.contentType()).orElse(serviceEndpoint.contentType());
  }
}
