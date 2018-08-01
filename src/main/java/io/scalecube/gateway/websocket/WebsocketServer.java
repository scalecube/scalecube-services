package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.tcp.BlockingNettyContext;

import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.BiFunction;

public final class WebsocketServer {

  private final WebsocketAcceptor acceptor;
  private BlockingNettyContext server;

  public WebsocketServer(Microservices microservices) {
    this.acceptor = new WebsocketAcceptor(microservices.call().create(), microservices.metrics());
  }

  public synchronized InetSocketAddress start() {
    return start(new InetSocketAddress("localhost", 0));
  }

  public synchronized InetSocketAddress start(int port) {
    return start(new InetSocketAddress("localhost", port));
  }

  public synchronized InetSocketAddress start(InetSocketAddress listenAddress) {
    server = HttpServer.builder().listenAddress(listenAddress).build().start(new WebSocketServerBiFunction());
    server.installShutdownHook();
    return server.getContext().address();
  }

  public synchronized void stop(Duration timeout) {
    if (server != null) {
      server.setLifecycleTimeout(timeout);
      server.shutdown();
    }
  }

  public synchronized void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  private class WebSocketServerBiFunction
      implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

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
