package io.scalecube.services;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServiceEndpoint {

  private String id;
  private String host;
  private int port;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceEndpoint() {}

  /**
   * Create a service endpoint.
   *
   * @param id the endpoint's id.
   * @param host the endpoint's host.
   * @param port the endpoint's port.
   * @param tags the endpoint's tags (if any).
   * @param serviceRegistrations registration for this endpoint.
   */
  public ServiceEndpoint(
      String id,
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

  /**
   * Return collection of service registratrions.
   *
   * @return collection of {@link ServiceRegistration}
   */
  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }

  /**
   * Creates collection of service references from this service endpoint.
   *
   * @return collection of {@link ServiceReference}
   */
  public Collection<ServiceReference> serviceReferences() {
    return serviceRegistrations
        .stream()
        .flatMap(sr -> sr.methods().stream().map(sm -> new ServiceReference(sm, sr, this)))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "ServiceEndpoint{"
        + "id='"
        + id
        + '\''
        + ", host='"
        + host
        + '\''
        + ", port="
        + port
        + ", tags="
        + tags
        + ", serviceRegistrations="
        + serviceRegistrations
        + '}';
  }
}
