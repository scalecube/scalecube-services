package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceReference;
import reactor.core.publisher.Mono;

public interface ClientTransport extends AutoCloseable {

  /**
   * Creates {@link ClientChannel} for communication with remote service endpoint.
   *
   * @param serviceReference target serviceReference
   * @return {@link ClientChannel} instance
   */
  ClientChannel create(ServiceReference serviceReference);

  /**
   * Supplier of credentials for authentication on the {@link ServerTransport}. Being used in the
   * connection setup phase with remote {@link ServerTransport}.
   */
  @FunctionalInterface
  interface CredentialsSupplier {

    /**
     * Obtains credentials for the given {@code serviceReference}.
     *
     * @param serviceReference target serviceReference
     * @return result
     */
    Mono<byte[]> credentials(ServiceReference serviceReference);
  }
}
