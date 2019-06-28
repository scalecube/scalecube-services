package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ClientTransport;
import io.scalecube.net.Address;

/** Factory low-level RSocket Transport. */
public interface RSocketClientTransportFactory {

  /**
   * Creates client transport for RSocket.
   *
   * @param address address remote service.
   * @return RSocket Client Transport
   */
  ClientTransport createClient(Address address);
}
