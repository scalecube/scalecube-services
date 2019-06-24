package io.scalecube.services.transport.api;

/** Service transport interface. */
public interface ServiceTransport<T extends TransportResources> {

  /**
   * Provider of service transport resources.
   *
   * @return service transport resources
   */
  T transportResources();

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
}
