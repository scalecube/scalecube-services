package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.StringJoiner;

public final class ServiceDiscoveryOptions implements Cloneable {

  private ServiceEndpoint serviceEndpoint;
  private ServiceDiscoveryFactory discoveryFactory;

  public ServiceDiscoveryOptions() {}

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public ServiceDiscoveryOptions serviceEndpoint(ServiceEndpoint serviceEndpoint) {
    final ServiceDiscoveryOptions c = clone();
    c.serviceEndpoint = serviceEndpoint;
    return c;
  }

  public ServiceDiscoveryFactory discoveryFactory() {
    return discoveryFactory;
  }

  public ServiceDiscoveryOptions discoveryFactory(ServiceDiscoveryFactory discoveryFactory) {
    final ServiceDiscoveryOptions c = clone();
    c.discoveryFactory = discoveryFactory;
    return c;
  }

  @Override
  public ServiceDiscoveryOptions clone() {
    try {
      return (ServiceDiscoveryOptions) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceDiscoveryOptions.class.getSimpleName() + "[", "]")
        .add("serviceEndpoint=" + serviceEndpoint)
        .add("discoveryFactory=" + discoveryFactory)
        .toString();
  }
}
