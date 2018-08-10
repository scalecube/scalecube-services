package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.server.api.ServerTransport;

import io.netty.channel.EventLoopGroup;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpServer;

public class RSocketServerTransport implements ServerTransport {

  private final ServiceMessageCodec codec;
  private final EventLoopGroup eventLoopGroup;

  private NettyContextCloseable server;

  public RSocketServerTransport(ServiceMessageCodec codec, EventLoopGroup eventLoopGroup) {
    this.codec = codec;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public InetSocketAddress bindAwait(InetSocketAddress address, ServiceMethodRegistry methodRegistry) {
    TcpServer tcpServer =
        TcpServer.create(options -> options
            .eventLoopGroup(eventLoopGroup)
            .listenAddress(address));

    this.server = RSocketFactory.receive()
        .frameDecoder(frame -> ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
        .transport(TcpServerTransport.create(tcpServer))
        .start()
        .block();

    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    eventLoopGroup.shutdownGracefully();

    if (server != null) {
      server.dispose();
      return server.onClose();
    } else {
      return Mono.empty();
    }
  }
}
