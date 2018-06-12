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

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import reactor.core.publisher.MonoProcessor;
import reactor.ipc.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Address, Publisher<RSocket>> rSockets = new ConcurrentHashMap<>();

  private final ServiceMessageCodec codec;

  public RSocketClientTransport(ServiceMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Publisher<RSocket>> monoMap = rSockets; // keep reference for threadsafety
    Publisher<RSocket> rSocket = monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketServiceClientAdapter(rSocket, codec);
  }

  private static Publisher<RSocket> connect(Address address, Map<Address, Publisher<RSocket>> monoMap) {
    MonoProcessor<RSocket> rSocketProcessor = MonoProcessor.create();

    RSocketFactory.connect()
        .transport(TcpClientTransport.create(
            TcpClient.create(options -> options
                .disablePool()
                .host(address.host())
                .port(address.port())
                .afterNettyContextInit(nettyContext -> {
                  // add handler to react on remote node closes connection
                  nettyContext.addHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) {
                      monoMap.remove(address);
                      LOGGER.info("Connection became inactive on {} and removed the pool", address);
                      ctx.fireChannelInactive();
                    }
                  });
                }))))
        .start()
        .subscribe(
            rSocket -> {
              LOGGER.info("Connected successfully on {}", address);
              rSocket.onClose().subscribe(aVoid -> {
                monoMap.remove(address);
                LOGGER.info("Connection closed on {} and removed from the pool", address);
              });
              rSocketProcessor.onNext(rSocket);
            },
            throwable -> {
              LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
              monoMap.remove(address);
              rSocketProcessor.onError(throwable);
            });

    return rSocketProcessor;
  }
}
