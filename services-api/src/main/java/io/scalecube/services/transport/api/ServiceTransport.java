package io.scalecube.services.transport.api;

import reactor.core.publisher.Mono;

/** Service transport interface. */
public interface ServiceTransport {

  /**
   * Provier of client transport.
   *
   * @return client transport
   */
  ClientTransport clientTransport();

  /**
   * Provider of server transport.
   *
   * @return server transport
   */
  ServerTransport serverTransport();

  /**
   * Starts service transport.
   *
   * @return mono result
   */
  Mono<? extends ServiceTransport> start();

  /**
   * Shutdowns service transport.
   *
   * @return shutdown completion signal
   */
  Mono<Void> stop();
}
