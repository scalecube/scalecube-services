package io.scalecube.gateway.rsocket.websocket;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.ByteBufPayload;
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
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.resources.LoopResources;

/**
 * Gateway implementation on RSocket over WebSocket.
 */
public class RSocketWebsocketGateway extends GatewayTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketGateway.class);

  private static final DefaultThreadFactory BOSS_THREAD_FACTORY =
    new DefaultThreadFactory("rsws-boss", true);

  private static final Duration START_TIMEOUT = Duration.ofSeconds(30);

  private NettyContextCloseable server;

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

          InetSocketAddress listenAddress = new InetSocketAddress(config.port());

          LoopResources loopResources =
            prepareLoopResources(preferNative, BOSS_THREAD_FACTORY, config, workerThreadPool);

          HttpServer httpServer =
              HttpServer.create(
                opts -> {
                  opts.listenAddress(listenAddress);
                  if (loopResources != null) {
                    opts.loopResources(loopResources);
                    }
                  });

          RSocketWebsocketAcceptor rSocketWebsocketAcceptor =
            new RSocketWebsocketAcceptor(call.create(), metrics);

          server =
              RSocketFactory.receive()
                  .frameDecoder(
                      frame ->
                          ByteBufPayload.create(
                              frame.sliceData().retain(), frame.sliceMetadata().retain()))
                .acceptor(rSocketWebsocketAcceptor)
                .transport(WebsocketServerTransport.create(httpServer))
                .start()
                .block(START_TIMEOUT);

          InetSocketAddress address = server.address();
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
            server.dispose();
            stopList.add(server.onClose());
          }
          return Mono.when(stopList);
        });
  }
}
