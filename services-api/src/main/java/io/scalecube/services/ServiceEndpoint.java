package io.scalecube.services;

import java.util.Collection;
import java.util.Map;

public class ServiceEndpoint {

  private String endpointId;
  private String host;
  private int port;
  private String contentType;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceEndpoint() {}

  public ServiceEndpoint(String endpointId,
      String host,
      int port,
      String contentType,
      Map<String, String> tags,
      Collection<ServiceRegistration> serviceRegistrations) {
    this.endpointId = endpointId;
    this.host = host;
    this.port = port;
    this.contentType = contentType;
    this.tags = tags;
    this.serviceRegistrations = serviceRegistrations;
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

  public String contentType() {
    return contentType;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }
}
