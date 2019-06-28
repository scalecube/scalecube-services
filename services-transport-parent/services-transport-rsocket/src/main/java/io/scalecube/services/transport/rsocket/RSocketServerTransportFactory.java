package io.scalecube.services.transport.rsocket;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.scalecube.net.Address;

/** Factory low-level transport for RSocket. */
public interface RSocketServerTransportFactory {

  /**
   * Creates a transport server and initializes it with the specified address.
   *
   * @param address address
   * @return RSocket Server Transport
   */
  ServerTransport<Server> createServerTransport(Address address);

  /** Adapter for closable server. Adds the ability to get the server address. */
  interface Server extends Closeable {

    /**
     * Return address of server.
     *
     * @return address of server.
     */
    Address address();
  }
}
