package io.scalecube.services.transport.api;

/** Service transport interface. */
public interface ServiceTransport {

  /**
   * Provier of client transport.
   *
   * @param resources service transport resources
   * @return client transport
   */
  ClientTransport clientTransport(TransportResources resources);

  /**
   * Provider of server transport.
   *
   * @param resources service transport resources
   * @return server transport
   */
  ServerTransport serverTransport(TransportResources resources);
}
