package io.scalecube.services;

import java.util.Collection;

import reactor.core.publisher.Mono;

/** Manages the life cycle of all services registered with Scale Cube Services. */
interface ServicesLifeCycleManager {

  /**
   * Create services.
   *
   * @param microservices scale cube instance.
   * @return collection of created services in Mono.
   */
  Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices);

  /**
   * Initializes instances of services.
   *
   * @param microservices scale cube instance.
   * @return Completed Mono if initialization was successful for all services.
   */
  Mono<Void> postConstruct(Microservices microservices);

  /**
   * Finalization of service instances.
   *
   * @param microservices scale cube instance.
   * @return completed Mono if finalization was successful for all services.
   */
  Mono<Microservices> shutDown(Microservices microservices);
}
