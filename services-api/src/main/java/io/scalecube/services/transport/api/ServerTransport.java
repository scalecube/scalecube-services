package io.scalecube.services.transport.api;

import io.scalecube.services.Address;

public interface ServerTransport {

  /**
   * Returns listening server address.
   *
   * @return listening server address, or {@code null} if {@link #bind()} was not called.
   */
  Address address();

  /**
   * Starts this instance.
   *
   * @return transport instance
   */
  ServerTransport bind();

  /** Stops this instance and release occupied resources. */
  void stop();
}
