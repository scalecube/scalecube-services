package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

public interface ServiceTransport {

  /**
   * Provier for {@link ClientTransport}.
   *
   * @param serviceEndpoint local serviceEndpoint
   * @param credentialsSupplier credentialsSupplier
   * @return {@code ClientTransport} instance
   */
  ClientTransport clientTransport(
      ServiceEndpoint serviceEndpoint, CredentialsSupplier credentialsSupplier);

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
