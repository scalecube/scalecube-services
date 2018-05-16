package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.codec.ServiceMessageCodec;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ThreadLocal<Map<Address, Mono<RSocket>>> rSockets = ThreadLocal.withInitial(ConcurrentHashMap::new);

  private final ServiceMessageCodec codec;

  public RSocketClientTransport(ServiceMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rSockets.get(); // Hint: keep reference for threadsafety
    Mono<RSocket> rSocket = monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketServiceClientAdapter(rSocket, codec);
  }

  private static Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    return RSocketFactory.connect()
        .transport(createTcpClientTransport(monoMap, address))
        .start()
        .doOnNext(rSocket -> {
          LOGGER.debug("Connected successfully on {}", address);
          rSocket.onClose().subscribe(aVoid -> {
            monoMap.remove(address);
            LOGGER.debug("Connection closed on {} and removed from the pool", address);
          });
        })
        .doOnError(throwable -> {
          LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
          monoMap.remove(address);
        });
  }

  private static TcpClientTransport createTcpClientTransport(Map<Address, Mono<RSocket>> monoMap, Address address) {
    return TcpClientTransport.create(
        TcpClient.create(options -> options
            .disablePool()
            .host(address.host())
            .port(address.port())
            .afterNettyContextInit(nettyContext -> nettyContext.addHandler(new ChannelInboundHandlerAdapter() {
              @Override
              public void channelInactive(ChannelHandlerContext ctx) {
                monoMap.remove(address);
                LOGGER.debug("Connection inactive on {} and removed from the pool", address);
                ctx.fireChannelInactive();
              }
            }))));
  }
}
