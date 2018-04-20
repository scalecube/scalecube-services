package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.transport.rsocket.RSocketJsonPayloadCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import io.rsocket.Payload;
import io.rsocket.transport.netty.server.NettyContextCloseable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class RSocketServerTransport implements ServerTransport {

  private NettyContextCloseable server;
  private ServiceMessageCodec<Payload> codec;
  private ServerMessageAcceptor acceptor;

  public RSocketServerTransport(ServiceMessageCodec<Payload> payloadCodec) {
    this.codec = payloadCodec;
  }

  @Override
  public ServerTransport accept(ServerMessageAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  @Override
  public InetSocketAddress bindAwait(InetSocketAddress address) {
    
    this.server = RSocketServerFactory.create(address, codec, acceptor)
        .start().block();
    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    server.dispose();
    return server.onClose();
  }

  @Override
  public Collection<? extends ServiceMessageCodec> availableServiceMessageCodec() {
    return Arrays.asList(
        new RSocketJsonPayloadCodec());
  }
}
