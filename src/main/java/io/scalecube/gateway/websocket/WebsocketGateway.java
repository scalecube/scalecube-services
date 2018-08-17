package io.scalecube.gateway.websocket;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.tcp.BlockingNettyContext;

import io.netty.channel.EventLoopGroup;

import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

public class WebsocketGateway implements Gateway {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketGateway.class);

  private BlockingNettyContext server;

  @Override
  public Mono<InetSocketAddress> start(GatewayConfig config,
      ExecutorService executorService,
      ServiceCall.Call call,
      Metrics metrics) {

    return Mono.defer(() -> {
      LOGGER.info("Starting gateway with {}", config);

      InetSocketAddress listenAddress = new InetSocketAddress(config.port());
      WebsocketAcceptor acceptor = new WebsocketAcceptor(call.create(), metrics);
      server = HttpServer.builder()
          .options(opts -> {
            opts.listenAddress(listenAddress);
            if (config.executorService() != null) {
              opts.eventLoopGroup((EventLoopGroup) config.executorService());
            } else if (executorService != null) {
              opts.eventLoopGroup((EventLoopGroup) executorService);
            }
          })
          .build()
          .start(new WebSocketServerBiFunction(acceptor));
      server.installShutdownHook();
      InetSocketAddress address = server.getContext().address();

      LOGGER.info("Gateway has been started successfully on {}", address);

      return Mono.just(address);
    });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.fromRunnable(() -> {
      if (server != null) {
        server.shutdown();
      }
    });
  }

  private static class WebSocketServerBiFunction
      implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

    private final WebsocketAcceptor acceptor;

    private WebSocketServerBiFunction(WebsocketAcceptor acceptor) {
      this.acceptor = acceptor;
    }

    @Override
    public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
      return httpResponse.sendWebsocket((WebsocketInbound inbound, WebsocketOutbound outbound) -> {
        WebsocketSession session = new WebsocketSession(httpRequest, inbound, outbound);
        Mono<Void> voidMono = acceptor.onConnect(session);
        session.onClose(() -> acceptor.onDisconnect(session));
        return voidMono;
      });
    }
  }
}
