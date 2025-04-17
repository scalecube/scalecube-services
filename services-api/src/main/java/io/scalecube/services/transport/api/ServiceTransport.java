package io.scalecube.services.transport.api;

import io.scalecube.services.registry.api.ServiceRegistry;

public interface ServiceTransport {

  /**
   * Provider for {@link ClientTransport}.
   *
   * @return {@link ClientTransport} instance
   */
  ClientTransport clientTransport();

  /**
   * Provider for {@link ServerTransport}.
   *
   * @param serviceRegistry {@link ServiceRegistry} instance
   * @return {@link ServerTransport} instance
   */
  ServerTransport serverTransport(ServiceRegistry serviceRegistry);

  /**
   * Starts {@link ServiceTransport} instance.
   *
   * @return started {@link ServiceTransport} instance
   */
  ServiceTransport start();

  /** Stops transport and release allocated transport resources. */
  void stop();
}
