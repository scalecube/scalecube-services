package io.scalecube.services.gateway.http;

import io.netty.channel.EventLoopGroup;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.gateway.GatewayLoopResources;
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

public class HttpGateway extends GatewayTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGateway.class);

  private DisposableServer server;
  private LoopResources loopResources;

  @Override
  public Mono<Gateway> start(
      GatewayConfig config, Executor workerPool, Call call, Metrics metrics) {

    return Mono.defer(
        () -> {
          LOGGER.info("Starting gateway with {}", config);

          GatewayMetrics metrics1 = new GatewayMetrics(config.name(), metrics);
          HttpGatewayAcceptor acceptor = new HttpGatewayAcceptor(call.create(), metrics1);

          if (workerPool != null) {
            loopResources = new GatewayLoopResources((EventLoopGroup) workerPool);
          }

          return prepareHttpServer(loopResources, config.port(), null /*metrics*/)
              .handle(acceptor)
              .bind()
              .doOnSuccess(server -> this.server = server)
              .doOnSuccess(
                  server ->
                      LOGGER.info(
                          "HTTP Gateway has been started successfully on {}", server.address()))
              .thenReturn(this);
        });
  }

  @Override
  public InetSocketAddress address() {
    return server.address();
  }

  @Override
  public Mono<Void> stop() {
    return shutdownServer(server).then(shutdownLoopResources(loopResources));
  }
}
