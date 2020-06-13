package io.scalecube.services;

import java.util.Collection;
import reactor.core.publisher.Mono;

/** Manages the life cycle of all services registered with Scalecube Services. */
public interface ServiceFactory {

  /**
   * Provide service definitions.
   *
   * @return collection of service definitions - service type and tags.
   * @see ServiceDefinition
   */
  Collection<ServiceDefinition> getServiceDefinitions();

  /**
   * Initialize instances of services.
   *
   * @param microservices microservices context
   * @return Completed Mono if initialization was successful for all services.
   */
  Mono<? extends Collection<ServiceInfo>> initializeServices(
      MicroservicesContext microservices);

  /**
   * Finalization of service instances.
   *
   * @param microservices microservices context
   * @return completed Mono if finalization was successful for all services.
   */
  default Mono<Void> shutdownServices(MicroservicesContext microservices) {
    return Mono.defer(Mono::empty);
  }
}
