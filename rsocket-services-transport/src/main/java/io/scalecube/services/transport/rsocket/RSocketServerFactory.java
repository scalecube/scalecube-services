package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;

public class RSocketServerFactory {

  private void start() {
    // provision a service on port 7000.
    RSocketFactory.receive().acceptor(new RSocketServiceMethodAcceptor())
        .transport(TcpServerTransport.create("localhost", 7000))
        .start()
        .subscribe();
  }
}
