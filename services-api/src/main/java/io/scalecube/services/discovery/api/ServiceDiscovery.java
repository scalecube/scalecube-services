package io.scalecube.services.discovery.api;

import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  /**
   * Returns service discovery address.
   *
   * @return discovery address
   */
  Address address();

  /**
   * Returns service endpoint.
   *
   * @return service endpoint
   */
  ServiceEndpoint serviceEndpoint();

  /**
   * Function to subscribe and listen on {@code ServiceDiscoveryEvent} events.
   *
   * @return stream of {@code ServiceDiscoveryEvent} events
   */
  Flux<ServiceDiscoveryEvent> listenDiscovery();

  /**
   * Starting this {@code ServiceDiscovery} instance.
   *
   * @return started {@code ServiceDiscovery} instance
   */
  Mono<ServiceDiscovery> start();

  /**
   * Shutting down this {@code ServiceDiscovery} instance.
   *
   * @return async signal of the result
   */
  Mono<Void> shutdown();
}
