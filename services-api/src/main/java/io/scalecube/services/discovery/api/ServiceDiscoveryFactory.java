package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;

@FunctionalInterface
public interface ServiceDiscoveryFactory {

  ServiceDiscovery createServiceDiscovery(ServiceEndpoint serviceEndpoint);
}
