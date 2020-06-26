package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.server.CloseableChannel;

public interface RSocketServerTransportFactory {

  ServerTransport<CloseableChannel> serverTransport();
}
