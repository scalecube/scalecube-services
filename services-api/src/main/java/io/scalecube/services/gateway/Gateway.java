package io.scalecube.services.gateway;

import io.scalecube.services.Address;
import reactor.core.publisher.Mono;

public interface Gateway {

  /**
   * Returns gateway id.
   *
   * @return gateway id
   */
  String id();

  /**
   * Returns gateway address.
   *
   * @return gateway listen address
   */
  Address address();

  /**
   * Starts the gateway.
   *
   * @return mono result
   */
  Mono<Gateway> start();

  /**
   * Stops the gateway.
   *
   * @return stop signal
   */
  Mono<Void> stop();
}
