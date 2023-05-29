package io.scalecube.services.discovery.api;

import io.scalecube.net.Address;
import reactor.core.publisher.Flux;

public interface ServiceDiscovery {

  /**
   * Return listening address.
   *
   * @return listening address, or null if {@link #start()} was not called.
   */
  Address address();

  /**
   * Function to subscribe and listen on stream of {@code ServiceDiscoveryEvent}\s.
   *
   * @return stream of {@code ServiceDiscoveryEvent}\s
   */
  Flux<ServiceDiscoveryEvent> listen();

  /**
   * Starts this {@link ServiceDiscovery} instance. After started - subscribers begin to receive
   * {@code ServiceDiscoveryEvent}\s on {@link #listen()}.
   */
  void start();

  /** Stops this {@link ServiceDiscovery} instance and release occupied resources. */
  void shutdown();
}
