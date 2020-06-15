package io.scalecube.services.discovery.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  /**
   * Function to subscribe and listen on {@code ServiceDiscoveryEvent} events.
   *
   * @return stream of {@code ServiceDiscoveryEvent} events
   */
  Flux<ServiceDiscoveryEvent> listen();

  /**
   * Starting this {@code ServiceDiscovery} instance.
   *
   * @return started {@code ServiceDiscovery} instance
   */
  Mono<Void> start();

  /**
   * Shutting down this {@code ServiceDiscovery} instance.
   *
   * @return async signal of the result
   */
  Mono<Void> shutdown();
}
