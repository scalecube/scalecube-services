package io.scalecube.gateway.websocket;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.GatewayTemplate;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.BlockingNettyContext;

public class WebsocketGateway extends GatewayTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketGateway.class);

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
      new DefaultThreadFactory("ws-boss", true);

  private static final Duration START_TIMEOUT = Duration.ofSeconds(30);

  private BlockingNettyContext server;

  @Override
  public Mono<InetSocketAddress> start(
      GatewayConfig config,
      Executor workerThreadPool,
      boolean preferNative,
      ServiceCall.Call call,
      Metrics metrics) {

    return Mono.defer(
        () -> {
          LOGGER.info("Starting gateway with {}", config);

          GatewayMetrics gatewayMetrics = new GatewayMetrics(config.name(), metrics);
          GatewayWebsocketAcceptor websocketAcceptor =
              new GatewayWebsocketAcceptor(call.create(), gatewayMetrics);

          server =
              prepareHttpServer(
                      prepareLoopResources(
                          preferNative, BOSS_THREAD_FACTORY, config, workerThreadPool),
                      gatewayMetrics,
                      config.port())
                  .start(websocketAcceptor, START_TIMEOUT);

          InetSocketAddress address = server.getContext().address();
          LOGGER.info("Gateway has been started successfully on {}", address);
          return Mono.just(address);
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          List<Mono<Void>> stopList = new ArrayList<>();
          stopList.add(shutdownBossGroup());
          if (server != null) {
            server.getContext().dispose();
            stopList.add(server.getContext().onClose());
          }
          return Mono.when(stopList);
        });
  }
}
