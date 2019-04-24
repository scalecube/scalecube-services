package io.scalecube.services.transport.api;

/** Client service transport interface. */
public interface ClientTransport {

  ClientTransport NO_CLIENT_TRANSPORT = new ClientTransport() {};

  /**
   * Creates a client channel ready for communication with remote service node.
   *
   * @param address address to connect
   * @return client channel instance.
   */
  default ClientChannel create(Address address) {
    return new ClientChannel() {};
  }
}
