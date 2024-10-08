package io.scalecube.services.discovery.api;

import io.scalecube.services.Address;
import reactor.core.publisher.Flux;

public interface ServiceDiscovery {

  /**
   * Return listening address.
   *
   * @return listening address, or null if {@link #start()} was not called.
   */
  Address address();

  /**
   * Function to subscribe and listen on service discovery stream.
   *
   * @return stream of {@code ServiceDiscoveryEvent} objects
   */
  Flux<ServiceDiscoveryEvent> listen();

  /** Starts this instance. */
  void start();

  /** Stops this instance and release occupied resources. */
  void shutdown();
}
