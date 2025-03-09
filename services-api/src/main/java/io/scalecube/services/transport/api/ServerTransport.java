package io.scalecube.services.transport.api;

import io.scalecube.services.Address;

public interface ServerTransport {

  /**
   * Returns listening server address.
   *
   * @return listening server address
   */
  Address address();

  /**
   * Starts this {@link ServerTransport} instance.
   *
   * @return started {@link ServerTransport} instance
   */
  ServerTransport bind();

  /** Stops this instance and release allocated resources. */
  void stop();
}
