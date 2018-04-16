package io.scalecube.services.transport.rsocket.server;

import io.rsocket.RSocketFactory;
import io.rsocket.RSocketFactory.Start;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;

public class RSocketServerFactory {

  static Start<NettyContextCloseable> create(int port) {
    return RSocketFactory.receive().acceptor(new RSocketServiceMethodAcceptor())
        .transport(TcpServerTransport.create(port));
  }
}
