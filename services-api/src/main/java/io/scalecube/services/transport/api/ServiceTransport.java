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
   * @param resources service transport resources
   * @return client transport
   */
  ClientTransport clientTransport(T resources);

  /**
   * Provider of server transport.
   *
   * @param resources service transport resources
   * @return server transport
   */
  ServerTransport serverTransport(T resources);
}
