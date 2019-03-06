package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

public interface ServiceDiscoveryFactory {

  /**
   * Factory for creating {@code ServiceDiscovery} instance.
   *
   * @param serviceRegistry service registry
   * @param serviceEndpoint service endpoint
   * @return {@code ServiceDiscovery} instance
   */
  ServiceDiscovery createFrom(ServiceRegistry serviceRegistry, ServiceEndpoint serviceEndpoint);
}
