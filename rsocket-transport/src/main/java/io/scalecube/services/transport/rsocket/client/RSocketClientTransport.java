package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ConcurrentMap<Address, Mono<RSocket>> rSockets = new ConcurrentHashMap<>();

  private final ServiceMessageCodec codec;

  public RSocketClientTransport(ServiceMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ClientChannel create(Address address) {
    // noinspection unchecked
    return new RSocketServiceClientAdapter(rSockets.computeIfAbsent(address, this::connect), codec);
  }

  @Override
  public ServiceMessageCodec getMessageCodec() {
    return codec;
  }

  private Mono<RSocket> connect(Address address) {
    CompletableFuture<RSocket> promise = new CompletableFuture<>();

    RSocketFactory.connect()
        .transport(createTcpClientTransport(address))
        .start()
        .subscribe(
            rSocket -> {
              LOGGER.debug("Connected successfully on {}", address);
              rSocket.onClose().subscribe(aVoid -> {
                rSockets.remove(address);
                LOGGER.debug("Connection closed on {} and removed from the pool", address);
              });
              promise.complete(rSocket);
            },
            throwable -> {
              LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
              rSockets.remove(address);
              promise.completeExceptionally(throwable);
            });

    return Mono.fromFuture(promise);
  }

  private TcpClientTransport createTcpClientTransport(Address address) {
    return TcpClientTransport.create(
        TcpClient.create(options -> options
            .disablePool()
            .host(address.host())
            .port(address.port())
            .afterNettyContextInit(nettyContext -> nettyContext.addHandler(new ChannelInboundHandlerAdapter() {
              @Override
              public void channelInactive(ChannelHandlerContext ctx) {
                rSockets.remove(address);
                LOGGER.debug("Connection inactive on {} and removed from the pool", address);
                ctx.fireChannelInactive();
              }
            }))));
  }
}
