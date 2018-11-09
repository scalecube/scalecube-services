package io.scalecube.services.gateway.ws;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.gateway.GatewayMetrics;
import io.scalecube.services.gateway.GatewayTemplate;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

public class WebsocketGateway extends GatewayTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketGateway.class);

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("ws-boss", true);

  private DisposableServer server;

  @Override
  public Mono<Gateway> start(
      GatewayConfig config,
      Executor workerThreadPool,
      boolean preferNative,
      ServiceCall.Call call,
      Metrics metrics) {

    return Mono.defer(
        () -> {
          LOGGER.info("Starting gateway with {}", config);

          GatewayMetrics metrics1 = new GatewayMetrics(config.name(), metrics);
          WebsocketGatewayAcceptor acceptor = new WebsocketGatewayAcceptor(call.create(), metrics1);

          LoopResources loopResources =
              prepareLoopResources(preferNative, BOSS_THREAD_FACTORY, config, workerThreadPool);

          return prepareHttpServer(loopResources, config.port(), metrics1)
              .handle(acceptor)
              .bind()
              .doOnSuccess(server -> this.server = server)
              .doOnSuccess(
                  server ->
                      LOGGER.info(
                          "Websocket Gateway has been started successfully on {}",
                          server.address()))
              .then(Mono.just(this));
        });
  }

  @Override
  public InetSocketAddress address() {
    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    return shutdownServer(server).then(shutdownBossGroup());
  }
}
