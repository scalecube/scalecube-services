package io.scalecube.gateway.rsocket.websocket;

import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.server.ExtendedWebsocketServerTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;

/**
 * Gateway implementation using RSocket protocol over WebSocket transport.
 *
 * @see io.rsocket.RSocket
 */
public class RSocketWebsocketGateway implements Gateway {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketGateway.class);

  private Mono<NettyContextCloseable> server;

  @Override
  public Mono<InetSocketAddress> start(
    GatewayConfig config,
    Executor selectorThreadPool,
    Executor workerThreadPool,
    Call call,
    Metrics metrics) {
    return Mono.defer(
        () -> {
          LOGGER.info("Starting gateway with {}", config);

          InetSocketAddress listenAddress = new InetSocketAddress(config.port());

          HttpServer httpServer =
              HttpServer.create(
                opts -> {
                  opts.listenAddress(listenAddress);
                  if (selectorThreadPool != null && workerThreadPool != null) {
                    opts.loopResources(
                      new RSocketWebsocketLoopResources(
                        true, selectorThreadPool, workerThreadPool));
                    }
                  });

          SocketAcceptor socketAcceptor = new RSocketWebsocketAcceptor(call.create(), metrics);

          server =
              RSocketFactory.receive()
                  .frameDecoder(
                      frame ->
                          ByteBufPayload.create(
                              frame.sliceData().retain(), frame.sliceMetadata().retain()))
                  .acceptor(socketAcceptor)
                .transport(new ExtendedWebsocketServerTransport(httpServer))
                  .start();

          return server
              .map(NettyContextCloseable::address)
              .doOnSuccess(
                  address -> LOGGER.info("Gateway has been started successfully on {}", address))
              .doOnError(throwable -> LOGGER.error("Gateway has NOT been started", throwable));
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          LOGGER.info("Stopping gateway...");

          return server
              .flatMap(
                  nettyContextCloseable -> {
                    nettyContextCloseable.dispose();
                    return nettyContextCloseable.onClose();
                  })
              .doOnSuccess(ignore -> LOGGER.info("Gateway has been stopped successfully"))
              .doOnError(
                  throwable ->
                      LOGGER.error("Exception occurred during gateway stopping", throwable));
        });
  }
}
