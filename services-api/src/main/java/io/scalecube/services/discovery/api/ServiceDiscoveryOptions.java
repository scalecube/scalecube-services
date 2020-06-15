package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Consumer;

public final class ServiceDiscoveryOptions {

  private String id = UUID.randomUUID().toString();
  private ServiceEndpoint serviceEndpoint;
  private ServiceDiscoveryFactory discoveryFactory;

  public ServiceDiscoveryOptions() {}

  /**
   * ServiceDiscoveryOptions copy constructor.
   *
   * @param other ServiceDiscoveryOptions to copy
   */
  public ServiceDiscoveryOptions(ServiceDiscoveryOptions other) {
    this.id = other.id;
    this.serviceEndpoint = other.serviceEndpoint;
    this.discoveryFactory = other.discoveryFactory;
  }

  private ServiceDiscoveryOptions set(Consumer<ServiceDiscoveryOptions> c) {
    ServiceDiscoveryOptions s = new ServiceDiscoveryOptions(this);
    c.accept(s);
    return s;
  }

  public ServiceDiscoveryOptions id(String id) {
    return set(o -> o.id = id);
  }

  public String id() {
    return id;
  }

  public ServiceDiscoveryOptions serviceEndpoint(ServiceEndpoint serviceEndpoint) {
    return set(o -> o.serviceEndpoint = serviceEndpoint);
  }

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public ServiceDiscoveryOptions discoveryFactory(ServiceDiscoveryFactory discoveryFactory) {
    return set(o -> o.discoveryFactory = discoveryFactory);
  }

  public ServiceDiscoveryFactory discoveryFactory() {
    return discoveryFactory;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceDiscoveryOptions.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("serviceEndpoint=" + serviceEndpoint)
        .add("discoveryFactory=" + discoveryFactory)
        .toString();
  }
}
