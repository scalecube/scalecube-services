package io.scalecube.services.transport.rsocket.server;

import io.rsocket.RSocketFactory;
import io.rsocket.RSocketFactory.Start;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

public class RSocketServerFactory {

  static Start<NettyContextCloseable> create(int port, PayloadCodec payloadCodec, ServerMessageAcceptor acceptor) {
    return RSocketFactory.receive().acceptor(new RSocketServiceMethodAcceptor(acceptor,payloadCodec))
        .transport(TcpServerTransport.create(port));
  }
}
