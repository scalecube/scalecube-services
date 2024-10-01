package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceReference;

public interface ClientTransport extends AutoCloseable {

  /**
   * Creates {@link ClientChannel} for communication with remote service endpoint.
   *
   * @param serviceReference target serviceReference
   * @return {@code ClientChannel} instance
   */
  ClientChannel create(ServiceReference serviceReference);
}
