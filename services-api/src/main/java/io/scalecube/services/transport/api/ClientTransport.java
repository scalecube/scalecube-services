package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceReference;

public interface ClientTransport {

  /**
   * Creates {@link ClientChannel} ready for communication with remote service endpoint.
   *
   * @param serviceReference target serviceReference
   * @return {@code ClientChannel} instance
   */
  ClientChannel create(ServiceReference serviceReference);
}
