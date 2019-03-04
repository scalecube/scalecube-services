package io.scalecube.services;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.registry.api.ServiceRegistry;
import reactor.core.publisher.Mono;

public interface ServiceDiscoveryFactory {

  Mono<ServiceDiscovery> createFrom(
      ServiceRegistry serviceRegistry, ServiceEndpoint serviceEndpoint);
}
