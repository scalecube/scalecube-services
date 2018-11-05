package io.scalecube.gateway;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public abstract class GatewayTemplate implements Gateway {

  private static final int BOSS_THREADS_NUM = 1;

  private EventLoopGroup bossGroup; // calculated

  /**
   * Builds loop resources object with given parameters. If gateway config contains thread pool then
   * this thread pool would be choosen as io worker thread pool, if not -- then service transprt
   * worker thread pool from parameters would be choosen as io worker thread pool. If config's
   * thread pool wasn't specified and service transpotr thread pool wasn't specified then this
   * function reutrns null.
   *
   * @param preferNative should native be preferred
   * @param bossThreadFactory connection acceptor factory
   * @param config gateway configuration object
   * @param workerThreadPool service transport worker thread pool
   * @return loop resources or null
   */
  protected final LoopResources prepareLoopResources(
      boolean preferNative,
      ThreadFactory bossThreadFactory,
      GatewayConfig config,
      Executor workerThreadPool) {

    EventLoopGroup workerGroup =
        (EventLoopGroup) Optional.ofNullable(config.workerThreadPool()).orElse(workerThreadPool);

    if (workerGroup == null) {
      return null;
    }

    bossGroup =
        preferNative
            ? new EpollEventLoopGroup(BOSS_THREADS_NUM, bossThreadFactory)
            : new NioEventLoopGroup(BOSS_THREADS_NUM, bossThreadFactory);

    return new GatewayLoopResources(preferNative, bossGroup, workerGroup);
  }

  /**
   * Builds generic http server with given parameters.
   *
   * @param loopResources loop resources calculated at {@link #prepareLoopResources(boolean,
   *     ThreadFactory, GatewayConfig, Executor)}
   * @param port listen port
   * @param metrics gateway metrics
   * @return http server
   */
  protected final HttpServer prepareHttpServer(
      LoopResources loopResources, int port, GatewayMetrics metrics) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              if (loopResources != null) {
                tcpServer = tcpServer.runOn(loopResources);
              }
              if (metrics != null) {
                tcpServer =
                    tcpServer.doOnConnection(
                        connection -> {
                          metrics.incConnection();
                          connection.onDispose(metrics::decConnection);
                        });
              }
              return tcpServer.addressSupplier(() -> new InetSocketAddress(port));
            });
  }

  /**
   * Shutting down boss thread pool if it's not null.
   *
   * @return mono handle
   */
  protected final Mono<Void> shutdownBossGroup() {
    //noinspection unchecked
    return Mono.defer(
        () ->
            bossGroup == null
                ? Mono.empty()
                : FutureMono.from((Future) ((EventLoopGroup) bossGroup).shutdownGracefully()));
  }
}
