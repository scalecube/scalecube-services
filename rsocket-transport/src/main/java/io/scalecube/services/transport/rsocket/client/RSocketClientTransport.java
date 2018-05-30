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
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Address, Mono<RSocket>> rSockets = new ConcurrentHashMap<>();

  private final ServiceMessageCodec codec;

  public RSocketClientTransport(ServiceMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rSockets; // keep reference for threadsafety
    Mono<RSocket> rSocket = monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketServiceClientAdapter(rSocket, codec);
  }

  private static Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    MonoProcessor<Scheduler> schedulerProcessor = MonoProcessor.create();
    MonoProcessor<RSocket> rSocketProcessor = MonoProcessor.create();

    RSocketFactory.connect()
        .transport(createTcpClientTransport(monoMap, address, schedulerProcessor))
        .start()
        .subscribe(
            rSocket -> {
              LOGGER.debug("Connected successfully on {}", address);
              rSocket.onClose().subscribe(aVoid -> {
                monoMap.remove(address);
                LOGGER.debug("Connection closed on {} and removed from the pool", address);
              });
              rSocketProcessor.onNext(rSocket);
              rSocketProcessor.onComplete();
            },
            throwable -> {
              LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
              monoMap.remove(address);
              rSocketProcessor.onError(throwable);
            });

    return schedulerProcessor.flatMap(rSocketProcessor::subscribeOn);
  }

  private static TcpClientTransport createTcpClientTransport(Map<Address, Mono<RSocket>> monoMap,
      Address address, MonoProcessor<Scheduler> schedulerProcessor) {

    return TcpClientTransport.create(
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
                  LOGGER.debug("Connection became inactive on {} and removed from monoMap", address);
                  ctx.fireChannelInactive();
                }
              });
              // expose executor where channel was assigned
              Scheduler scheduler = Schedulers.fromExecutor(nettyContext.channel().eventLoop());
              schedulerProcessor.onNext(scheduler);
              schedulerProcessor.onComplete();
              LOGGER.debug("Obtained scheduler {} on channel {}", scheduler, nettyContext.channel());
            })));
  }
}
