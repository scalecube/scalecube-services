package io.scalecube.services;

import io.scalecube.services.api.Qualifier;
import io.scalecube.transport.Address;

import java.util.HashMap;
import java.util.Map;

public class ServiceReference {

  private String qualifier;
  private String endpointId;
  private String host;
  private int port;
  private String namespace;
  private String contentType;
  private Map<String, String> tags;
  private String action;
  private CommunicationMode mode;
  private Address address;

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
    this.mode = serviceMethodDefinition.getCommunicationMode();
    this.qualifier = Qualifier.asString(namespace, action);
    this.address = Address.create(this.host(), this.port());
  }

  public CommunicationMode mode() {
    return mode;
  }

  public String qualifier() {
    return this.qualifier;
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

  @Override
  public String toString() {
    return "ServiceReference{" +
        "qualifier='" + qualifier + '\'' +
        ", endpointId='" + endpointId + '\'' +
        ", host='" + host + '\'' +
        ", port=" + port +
        ", namespace='" + namespace + '\'' +
        ", contentType='" + contentType + '\'' +
        ", tags=" + tags +
        ", action='" + action + '\'' +
        ", mode=" + mode +
        '}';
  }

  public Address address() {
    return this.address;
  }
}
