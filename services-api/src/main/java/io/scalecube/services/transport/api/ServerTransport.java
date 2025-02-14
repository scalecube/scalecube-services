package io.scalecube.services.transport.api;

import io.scalecube.services.Address;
import reactor.core.publisher.Mono;

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

  /**
   * Authentication interface to handle clients that being connected from remote {@link
   * ClientTransport} instances.
   */
  @FunctionalInterface
  interface Authenticator {

    /**
     * Authenticates service transport connection by given credentials.
     *
     * @param credentials credentials
     * @return result
     */
    Mono<Object> authenticate(byte[] credentials);
  }
}
