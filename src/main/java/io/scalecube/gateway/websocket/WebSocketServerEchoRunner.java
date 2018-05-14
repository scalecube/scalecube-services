package io.scalecube.gateway.websocket;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public class WebSocketServerEchoRunner {

  public static void main(String[] args) throws InterruptedException {
    WebSocketServer server = new WebSocketServer(new WebSocketAcceptor() {
      @Override
      public Mono<Void> onConnect(WebSocketSession session) {
        return session.send(session.receive());
      }

      @Override
      public Mono<Void> onDisconnect(WebSocketSession session) {
        return Mono.empty();
      }
    });
    server.start(new InetSocketAddress(8080));
    Thread.currentThread().join();
  }
}
