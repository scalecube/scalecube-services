package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.ExtendedTcpServerTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.tcp.TcpServer;

public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private final ServiceMessageCodec codec;
  private final LoopResources loopResources;

  private NettyContextCloseable server; // calculated
  private List<NettyContext> channels = new CopyOnWriteArrayList<>(); // calculated

  public RSocketServerTransport(ServiceMessageCodec codec, LoopResources loopResources) {
    this.codec = codec;
    this.loopResources = loopResources;
  }

  @Override
  public InetSocketAddress bindAwait(
      InetSocketAddress address, ServiceMethodRegistry methodRegistry) {
    TcpServer tcpServer =
        TcpServer.create(
            options ->
                options
                    .loopResources(loopResources)
                    .listenAddress(address)
                    .afterNettyContextInit(
                        nettyContext -> {
                          LOGGER.info("Accepted connection on {}", nettyContext.channel());
                          nettyContext.onClose(
                              () -> {
                                LOGGER.info("Connection closed on {}", nettyContext.channel());
                                channels.remove(nettyContext);
                              });
                          channels.add(nettyContext);
                        }));

    this.server =
        RSocketFactory.receive()
            .frameDecoder(
                frame ->
                    ByteBufPayload.create(
                        frame.sliceData().retain(), frame.sliceMetadata().retain()))
            .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
            .transport(new ExtendedTcpServerTransport(tcpServer))
            .start()
            .block();

    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    if (server != null) {
      server.dispose();

      List<Mono<Void>> onCloseList =
          channels
              .stream()
              .map(
                  nettyContext -> {
                    nettyContext.dispose();
                    return nettyContext.onClose();
                  })
              .collect(Collectors.toList());

      return server.onClose().then(Mono.when(onCloseList));
    } else {
      return Mono.empty();
    }
  }
}
