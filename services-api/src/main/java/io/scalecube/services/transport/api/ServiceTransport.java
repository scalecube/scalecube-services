package io.scalecube.services.transport.api;

import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

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
   * Starts transport.
   *
   * @return mono result
   */
  Mono<? extends ServiceTransport> start();

  /**
   * Shutdowns transport.
   *
   * @return shutdown completion signal
   */
  Mono<Void> stop();
}
