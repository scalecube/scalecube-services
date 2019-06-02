package io.scalecube.services.gateway;

import java.net.InetSocketAddress;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public abstract class GatewayTemplate implements Gateway {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayTemplate.class);

  /**
   * Builds generic http server with given parameters.
   *
   * @param loopResources loop resources
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
   * Shutting down loopResources if it's not null.
   *
   * @return mono handle
   */
  protected final Mono<Void> shutdownLoopResources(LoopResources loopResources) {
    return Mono.defer(
        () ->
            Optional.ofNullable(loopResources)
                .map(
                    lr ->
                        lr.disposeLater()
                            .doOnError(
                                e -> LOGGER.warn("Failed to close gateway loopResources: " + e))
                            .onErrorResume(e -> Mono.empty()))
                .orElse(Mono.empty()));
  }

  /**
   * Shutting down server of type {@link DisposableServer} if it's not null.
   *
   * @param disposableServer server
   * @return mono hanle
   */
  protected final Mono<Void> shutdownServer(DisposableServer disposableServer) {
    return Mono.defer(
        () ->
            Optional.ofNullable(disposableServer)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onDispose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e))
                          .onErrorResume(e -> Mono.empty());
                    })
                .orElse(Mono.empty()));
  }
}
