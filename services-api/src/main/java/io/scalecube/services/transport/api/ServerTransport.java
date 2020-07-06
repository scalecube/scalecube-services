package io.scalecube.services.transport.api;

import io.scalecube.net.Address;
import reactor.core.publisher.Mono;

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
   * @return bound server address
   */
  Mono<ServerTransport> bind();

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
