package io.scalecube.services.transport.api;

/**
 * Provide Client&amp;Server Transports.
 *
 * @param <T> - transport resource for creation transports
 */
public interface ServiceTransportFactory<T extends TransportResources<T>> {

  /**
   * Provider of client transport.
   *
   * @param resources service transport resources
   * @return client transport
   */
  ClientTransport<T> clientTransport(T resources);

  /**
   * Provider of server transport.
   *
   * @param resources service transport resources
   * @return server transport
   */
  ServerTransport<T> serverTransport(T resources);
}
