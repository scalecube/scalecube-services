package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.codecs.api.MessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;
import io.scalecube.services.transport.server.api.ServerTransport;

import io.rsocket.Payload;
import io.rsocket.transport.netty.server.NettyContextCloseable;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public class RSocketServerTransport implements ServerTransport {

  private NettyContextCloseable server;
  private MessageCodec codec;
  private ServerMessageAcceptor acceptor;

  public RSocketServerTransport(MessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ServerTransport accept(ServerMessageAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  @Override
  public InetSocketAddress bindAwait(InetSocketAddress address) {

    this.server = RSocketServerFactory.create(address, (ServiceMessageCodec) codec, acceptor)
        .start().block();
    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    if (server != null) {
      server.dispose();
      return server.onClose();
    } else {
      return Mono.empty();
    }
  }
}
