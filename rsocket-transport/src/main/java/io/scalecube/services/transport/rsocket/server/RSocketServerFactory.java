package io.scalecube.services.transport.rsocket.server;


import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.RSocketFactory.Start;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;

public class RSocketServerFactory {

  static Start<NettyContextCloseable> create(int port,ServiceMessageCodec<Payload> codec, ServerMessageAcceptor acceptor) {
    
    TcpServerTransport transport = TcpServerTransport.create(port);

    return RSocketFactory.receive()
        .acceptor(new RSocketServiceMethodAcceptor(acceptor,codec))
        .transport(transport);

  }
}
