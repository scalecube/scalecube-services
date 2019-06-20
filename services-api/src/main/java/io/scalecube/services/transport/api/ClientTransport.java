package io.scalecube.services.transport.api;

import io.scalecube.net.Address;

/**
 * Client service transport interface.
 *
 * @param <T> - transport resource for this ClientTransport
 */
public interface ClientTransport<T extends TransportResources<T>> {

  /**
   * Creates a client channel ready for communication with remote service node.
   *
   * @param address address to connect
   * @return client channel instance.
   */
  ClientChannel create(Address address);
}
