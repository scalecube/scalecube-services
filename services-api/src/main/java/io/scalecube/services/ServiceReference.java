package io.scalecube.services;

import java.util.HashMap;
import java.util.Map;

public class ServiceReference {

  @Override
  public String toString() {
    return "ServiceReference [endpointId=" + endpointId + ", host=" + host + ", port=" + port + ", namespace="
        + namespace + ", contentType=" + contentType + ", tags=" + tags + ", action=" + action + "]";
  }

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

  public ServiceReference(ServiceMethodDefinition serviceMethodDefinition,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    this.endpointId = serviceEndpoint.id();
    this.host = serviceEndpoint.host();
    this.port = serviceEndpoint.port();
    this.namespace = serviceRegistration.namespace();
    this.contentType = mergeContentType(serviceMethodDefinition, serviceRegistration);
    this.tags = mergeTags(serviceMethodDefinition, serviceRegistration, serviceEndpoint);
    this.action = serviceMethodDefinition.getAction();
  }

  public ServiceReference(String endpointId, String host, int port, String namespace, String contentType, Map<String, String> tags, String action) {
    this.endpointId = endpointId;
    this.host = host;
    this.port = port;
    this.namespace = namespace;
    this.contentType = contentType;
    this.tags = tags;
    this.action = action;
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

  private Map<String, String> mergeTags(ServiceMethodDefinition serviceMethodDefinition,
      ServiceRegistration serviceRegistration,
      ServiceEndpoint serviceEndpoint) {
    Map<String, String> tags = new HashMap<>();
    tags.putAll(serviceEndpoint.tags());
    tags.putAll(serviceRegistration.tags());
    tags.putAll(serviceMethodDefinition.getTags());
    return tags;
  }

  private String mergeContentType(ServiceMethodDefinition serviceMethodDefinition,
                                  ServiceRegistration serviceRegistration) {
    if (serviceMethodDefinition.getContentType() != null && !serviceMethodDefinition.getContentType().isEmpty()) {
      return serviceMethodDefinition.getContentType();
    }
    if (serviceRegistration.contentType() != null && !serviceRegistration.contentType().isEmpty()) {
      return serviceRegistration.contentType();
    }
    throw new IllegalArgumentException();
  }
}
