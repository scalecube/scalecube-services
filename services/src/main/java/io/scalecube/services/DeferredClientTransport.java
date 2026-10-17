package io.scalecube.services;

import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;

/**
 * {@link ClientTransport} of a {@link Microservices.Context} that exists before the microservices
 * instance is started. {@link Microservices} binds it to its real client transport on start and
 * unbinds it on close; while unbound, creating a channel fails with {@link
 * ServiceUnavailableException}.
 */
final class DeferredClientTransport implements ClientTransport {

  private volatile ClientTransport delegate;

  void bind(ClientTransport clientTransport) {
    delegate = clientTransport;
  }

  void unbind() {
    delegate = null;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    final var clientTransport = delegate;
    if (clientTransport == null) {
      throw new ServiceUnavailableException("Microservices is not started");
    }
    return clientTransport.create(serviceReference);
  }

  @Override
  public void close() {
    // the bound client transport is owned and closed by Microservices
  }
}
