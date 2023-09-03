package io.scalecube.services.transport.api;

import io.scalecube.services.registry.api.ServiceRegistry;

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
   * @param serviceRegistry serviceRegistry
   * @return {@code ServerTransport} instance
   */
  ServerTransport serverTransport(ServiceRegistry serviceRegistry);

  /**
   * Starts {@link ServiceTransport} instance.
   *
   * @return transport instance
   */
  ServiceTransport start();

  /** Shutdowns transport and release occupied resources. */
  void stop();
}
