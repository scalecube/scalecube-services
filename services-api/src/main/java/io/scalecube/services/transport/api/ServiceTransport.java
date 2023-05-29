package io.scalecube.services.transport.api;

import io.scalecube.services.methods.ServiceMethodRegistry;

public interface ServiceTransport {

  /**
   * Provider for {@link ClientTransport}.
   *
   * @return {@code ClientTransport} instance
   */
  ClientTransport clientTransport();

  /**
   * Provider for {@link ServerTransport}.
   *
   * @param methodRegistry methodRegistry
   * @return {@code ServerTransport} instance
   */
  ServerTransport serverTransport(ServiceMethodRegistry methodRegistry);

  /**
   * Starts {@link ServiceTransport} instance.
   *
   * @return transport instance
   */
  ServiceTransport start();

  /** Shutdowns transport and release occupied resources. */
  void stop();
}
