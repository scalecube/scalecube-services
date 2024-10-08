package io.scalecube.services.gateway;

import io.scalecube.services.Address;

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
   * Starts gateway.
   *
   * @return gateway instance
   */
  Gateway start();

  /** Stops gateway. */
  void stop();
}
