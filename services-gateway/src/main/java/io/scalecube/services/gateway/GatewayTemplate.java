package io.scalecube.services.gateway;

import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public abstract class GatewayTemplate implements Gateway {

  protected final GatewayOptions options;

  protected GatewayTemplate(GatewayOptions options) {
    this.options =
        new GatewayOptions()
            .id(options.id())
            .port(options.port())
            .workerPool(options.workerPool())
            .call(options.call());
  }

  @Override
  public final String id() {
    return options.id();
  }

  /**
   * Builds generic http server with given parameters.
   *
   * @param loopResources loop resources
   * @param port listen port
   * @return http server
   */
  protected HttpServer prepareHttpServer(LoopResources loopResources, int port) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              if (loopResources != null) {
                tcpServer = tcpServer.runOn(loopResources);
              }
              return tcpServer.bindAddress(() -> new InetSocketAddress(port));
            });
  }

  /**
   * Shutting down loopResources if it's not null.
   *
   * @return mono handle
   */
  protected final Mono<Void> shutdownLoopResources(LoopResources loopResources) {
    return Mono.defer(
        () -> {
          if (loopResources == null) {
            return Mono.empty();
          }
          return loopResources.disposeLater();
        });
  }

  /**
   * Shutting down server of type {@link DisposableServer} if it's not null.
   *
   * @param server server
   * @return mono hanle
   */
  protected final Mono<Void> shutdownServer(DisposableServer server) {
    return Mono.defer(
        () -> {
          if (server == null) {
            return Mono.empty();
          }
          server.dispose();
          return server.onDispose();
        });
  }
}
