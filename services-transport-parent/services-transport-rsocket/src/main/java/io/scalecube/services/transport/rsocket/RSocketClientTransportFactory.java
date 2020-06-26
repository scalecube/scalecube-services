package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ClientTransport;
import io.scalecube.net.Address;

public interface RSocketClientTransportFactory {

  ClientTransport clientTransport(Address address);
}
