package io.scalecube.services.transport.api;

/** Client service transport interface. */
public interface ClientTransport {

  /**
   * Creates a client channel ready for communication with remote service node.
   *
   * @param address address to connect
   * @return client channel instance.
   */
  ClientChannel create(Address address);
}
