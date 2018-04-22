package io.scalecube.services.transport.rsocket.server;


import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import io.rsocket.RSocketFactory;
import io.rsocket.RSocketFactory.Start;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;

import java.net.InetSocketAddress;

public class RSocketServerFactory {

  static Start<NettyContextCloseable> create(InetSocketAddress addrress, ServiceMessageCodec codec, ServerMessageAcceptor acceptor) {
    
    TcpServerTransport transport = TcpServerTransport.create(addrress);

    return RSocketFactory.receive()
        .acceptor(new RSocketServiceMethodAcceptor(acceptor,codec))
        .transport(transport);

  }
}
