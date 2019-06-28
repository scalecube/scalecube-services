package io.scalecube.services.transport.api;

/** Scalecube Transport Provider. */
public interface ServiceTransportProvider {

  /**
   * Provide client transport factory.
   *
   * @return client transport factory
   */
  ClientTransportFactory provideClientTransportFactory();

  /**
   * Provide server transport.
   *
   * @return server transport
   */
  ServerTransport provideServerTransport();
}
