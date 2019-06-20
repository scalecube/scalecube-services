package io.scalecube.services.transport.api;

import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

/**
 * Server service transport interface.
 *
 * @param <T> - transport resource for ServerTransport
 */
public interface ServerTransport<T extends TransportResources<T>> {

  /**
   * Returns factual listen server address.
   *
   * @return listen server address
   */
  Address address();

  /**
   * Starts a server transport.
   *
   * @param port           listen port (can be {@code 0})
   * @param methodRegistry service method registry
   * @return bound server address
   */
  Mono<ServerTransport<T>> bind(int port, ServiceMethodRegistry methodRegistry);

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
