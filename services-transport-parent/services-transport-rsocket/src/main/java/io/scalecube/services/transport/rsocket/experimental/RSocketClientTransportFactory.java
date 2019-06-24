package io.scalecube.services.transport.rsocket.experimental;

import io.rsocket.transport.ClientTransport;
import io.scalecube.net.Address;

public interface RSocketClientTransportFactory {

  ClientTransport createClient(Address address);
}
