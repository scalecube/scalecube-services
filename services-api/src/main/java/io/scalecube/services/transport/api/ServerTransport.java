package io.scalecube.services.transport.api;

import io.scalecube.net.Address;

public interface ServerTransport {

  /**
   * Returns listening server address.
   *
   * @return listening server address, or {@code null} if {@link #bind()} was not called.
   */
  Address address();

  /**
   * Starts {@link ServiceTransport} instance.
   *
   * @return transport instance
   */
  ServerTransport bind();

  /** Stops this {@link ServiceTransport} instance and release occupied resources. */
  void stop();
}
