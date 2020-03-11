package io.scalecube.services;

import java.util.Collection;

import reactor.core.publisher.Mono;

/**
 * Type alias for {@link ServicesLifeCycleManager}, for better display of semantics.
 */
public interface ServicesProvider extends ServicesLifeCycleManager {

  /**
   * Method alias for {@link ServicesLifeCycleManager#constructServices(Microservices)}.
   *
   * @param microservices scale cube instance.
   * @return collection services.
   */
  default Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices) {
    return this.constructServices(microservices);
  }
}
