package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.Map;
import java.util.function.Function;
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
   * @param serviceRegistry {@link ServiceRegistry} instance
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

  /**
   * Returns credentials for the given {@link ServiceReference}. Credentials are being returned in
   * most generic form which is {@code Map<String, String>}.
   */
  @FunctionalInterface
  interface CredentialsSupplier extends Function<ServiceReference, Mono<Map<String, String>>> {}
}
