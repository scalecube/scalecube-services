package io.scalecube.services.gateway;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.registry.api.ServiceRegistry;

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
   * @param call {@link ServiceCall} instance
   * @param serviceRegistry {@link ServiceRegistry} instance
   * @return gateway instance
   */
  Gateway start(ServiceCall call, ServiceRegistry serviceRegistry);

  /** Stops gateway. */
  void stop();
}
