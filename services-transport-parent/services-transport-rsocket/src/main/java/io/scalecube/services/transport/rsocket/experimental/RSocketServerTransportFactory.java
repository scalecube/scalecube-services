package io.scalecube.services.transport.rsocket.experimental;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.scalecube.net.Address;
import java.net.InetSocketAddress;

public interface RSocketServerTransportFactory {

  ServerTransport<? extends Closeable> createServer(Address address);

}
