package io.scalecube.services;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ServiceEndpoint {

  private String id;
  private String host;
  private int port;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceEndpoint() {}

  public ServiceEndpoint(String id,
      String host,
      int port,
      Set<String> contentTypes,
      Map<String, String> tags,
      Collection<ServiceRegistration> serviceRegistrations) {
    this.id = id;
    this.host = host;
    this.port = port;
    this.contentTypes = contentTypes;
    this.tags = tags;
    this.serviceRegistrations = serviceRegistrations;
  }

  public String id() {
    return id;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public Set<String> contentTypes() {
    return contentTypes;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }

  @Override
  public String toString() {
    return "ServiceEndpoint{" +
        "id='" + id + '\'' +
        ", host='" + host + '\'' +
        ", port=" + port +
        ", tags=" + tags +
        ", serviceRegistrations=" + serviceRegistrations +
        '}';
  }
}
