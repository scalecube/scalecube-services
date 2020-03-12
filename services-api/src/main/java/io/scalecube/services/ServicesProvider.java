package io.scalecube.services;

import java.util.Collection;

import reactor.core.publisher.Mono;

/** Manages the life cycle of all services registered with Scale Cube Services. */
public interface ServicesProvider {

  /**
   * Provide service definitions.
   *
   * @param microservices scale cube instance.
   * @return collection of service definitions - service type&amp;tags.
   * @see ServiceDefinition
   */
  Mono<? extends Collection<ServiceDefinition>> provideServiceDefinitions(
      Microservices microservices);

  /**
   * Initialize instances of services.
   *
   * @param microservices scale cube instance.
   * @return Completed Mono if initialization was successful for all services.
   */
  Mono<? extends Collection<ServiceInfo>> provideService(Microservices microservices);

  /**
   * Finalization of service instances.
   *
   * @param microservices scale cube instance.
   * @return completed Mono if finalization was successful for all services.
   */
  Mono<Microservices> shutDown(Microservices microservices);
}
