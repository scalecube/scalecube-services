package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.ExtendedTcpServerTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.FutureMono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.tcp.TcpServer;

/**
 * RSocket server transport implementation.
 */
public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private static final int BOSS_THREADS_NUM = 1;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("rsocket-boss", true);

  private final ServiceMessageCodec codec;
  private final EventLoopGroup bossGroup;
  private final DelegatedLoopResources loopResources;

  private NettyContextCloseable server; // calculated
  private List<NettyContext> channels = new CopyOnWriteArrayList<>(); // calculated

  /**
   * Constructor for this server transport.
   *
   * @param codec message codec
   * @param preferEpoll should epoll be preferred
   * @param eventLoopGroup worker thread pool
   */
  public RSocketServerTransport(
      ServiceMessageCodec codec, boolean preferEpoll, EventLoopGroup eventLoopGroup) {
    this.codec = codec;

    this.bossGroup =
        preferEpoll
            ? new EpollEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY)
            : new NioEventLoopGroup(BOSS_THREADS_NUM, BOSS_THREAD_FACTORY);

    this.loopResources = new DelegatedLoopResources(preferEpoll, bossGroup, eventLoopGroup);
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
    return Mono.defer(
        () -> {
          List<Mono<Void>> stopList = new ArrayList<>();

          //noinspection unchecked
          stopList.add(FutureMono.from((Future) ((EventLoopGroup) bossGroup).shutdownGracefully()));

          channels
              .stream()
              .collect(
                  () -> stopList,
                  (list, context) -> {
                    context.dispose();
                    list.add(context.onClose());
                  },
                  (monos1, monos2) -> {
                    // no-op
                  });

          if (server != null) {
            server.dispose();
            stopList.add(server.onClose());
          }

          return Mono.when(stopList);
        });
  }
}
