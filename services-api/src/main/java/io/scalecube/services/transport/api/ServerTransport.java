package io.scalecube.services.transport.api;

import io.scalecube.net.Address;
import io.scalecube.services.auth.AuthContextRegistry;
import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

/** Server service transport interface. */
public interface ServerTransport {

  /**
   * Returns factual listen server address.
   *
   * @return listen server address
   */
  Address address();

  /**
   * Starts a server transport.
   *
   * @param methodRegistry service method registry
   * @param authContextRegistry auth context registry
   * @return bound server address
   */
  Mono<ServerTransport> bind(
      ServiceMethodRegistry methodRegistry, AuthContextRegistry authContextRegistry);

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
