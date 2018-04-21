package io.scalecube.services;

import java.util.HashMap;
import java.util.Map;

public class ServiceReference {

  private String endpointId;
  private String host;
  private int port;
  private String namespace;
  private String contentType;
  private Map<String, String> tags;
  private String action;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceReference() {}

  public ServiceReference(ServiceMethod serviceMethod,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    this.endpointId = serviceEndpoint.endpointId();
    this.host = serviceEndpoint.host();
    this.port = serviceEndpoint.port();
    this.namespace = serviceRegistration.namespace();
    this.contentType = mergeContentType(serviceMethod, serviceRegistration, serviceEndpoint);
    this.tags = mergeTags(serviceMethod, serviceRegistration, serviceEndpoint);
    this.action = serviceMethod.action();
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

  public String action() {
    return action;
  }

  private Map<String, String> mergeTags(ServiceMethod serviceMethod,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    Map<String, String> tags = new HashMap<>();
    tags.putAll(serviceEndpoint.tags());
    tags.putAll(serviceRegistration.tags());
    tags.putAll(serviceMethod.tags());
    return tags;
  }

  private String mergeContentType(ServiceMethod serviceMethod,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    if (serviceMethod.contentType() != null && !serviceMethod.contentType().isEmpty()) {
      return serviceMethod.contentType();
    }
    if (serviceRegistration.contentType() != null && !serviceRegistration.contentType().isEmpty()) {
      return serviceRegistration.contentType();
    }
    if (serviceEndpoint.contentType() != null && !serviceEndpoint.contentType().isEmpty()) {
      return serviceEndpoint.contentType();
    }
    throw new IllegalArgumentException();
  }
}
