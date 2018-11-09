package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.netty.tcp.TcpServer;

/** RSocket server transport implementation. */
public class RSocketServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServerTransport.class);

  private static final int BOSS_THREADS_NUM = 1;

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("rsocket-boss", true);

  private final ServiceMessageCodec codec;
  private final EventLoopGroup bossGroup;
  private final DelegatedLoopResources loopResources;

  private CloseableChannel server; // calculated
  private List<Connection> connections = new CopyOnWriteArrayList<>(); // calculated

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
  public Mono<InetSocketAddress> bind(int port, ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () -> {
          TcpServer tcpServer =
              TcpServer.create()
                  .runOn(loopResources)
                  .addressSupplier(() -> new InetSocketAddress(port))
                  .doOnConnection(
                      connection -> {
                        LOGGER.info("Accepted connection on {}", connection.channel());
                        connection.onDispose(
                            () -> {
                              LOGGER.info("Connection closed on {}", connection.channel());
                              connections.remove(connection);
                            });
                        connections.add(connection);
                      });

          return RSocketFactory.receive()
              .frameDecoder(
                  frame ->
                      ByteBufPayload.create(
                          frame.sliceData().retain(), frame.sliceMetadata().retain()))
              .acceptor(new RSocketServiceAcceptor(codec, methodRegistry))
              .transport(() -> TcpServerTransport.create(tcpServer))
              .start()
              .map(server -> this.server = server)
              .map(CloseableChannel::address);
        });
  }

  @Override
  public Mono<Void> stop() {
    return shutdownServer(server).then(closeConnections()).then(shutdownBossGroup());
  }

  private Mono<Void> closeConnections() {
    return Mono.defer(
        () ->
            Mono.when(
                    connections
                        .stream()
                        .map(
                            connection -> {
                              connection.dispose();
                              return connection
                                  .onTerminate()
                                  .doOnError(e -> LOGGER.warn("Failed to close connection: " + e))
                                  .onErrorResume(e -> Mono.empty());
                            })
                        .collect(Collectors.toList()))
                .doOnTerminate(connections::clear));
  }

  private Mono<Void> shutdownBossGroup() {
    return Mono.defer(
        () -> {
          //noinspection unchecked
          return Optional.ofNullable(bossGroup)
              .map(
                  bossGroup ->
                      FutureMono.from((Future<Void>) bossGroup.shutdownGracefully())
                          .doOnError(e -> LOGGER.warn("Failed to close bossGroup: " + e))
                          .onErrorResume(e -> Mono.empty()))
              .orElse(Mono.empty());
        });
  }

  private Mono<Void> shutdownServer(CloseableChannel closeableChannel) {
    return Mono.defer(
        () ->
            Optional.ofNullable(closeableChannel)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onClose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e))
                          .onErrorResume(e -> Mono.empty());
                    })
                .orElse(Mono.empty()));
  }
}
