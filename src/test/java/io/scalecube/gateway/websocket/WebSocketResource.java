package io.scalecube.gateway.websocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.rules.ExternalResource;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;

public class WebSocketResource extends ExternalResource implements Closeable {

  private WebSocketServer server;
  private InetSocketAddress serverAddress;

  public InetSocketAddress newServer(Function<WebSocketSession, Mono<Void>> onConnect,
      Function<WebSocketSession, Mono<Void>> onDisconnect) {
    server = new WebSocketServer(new WebSocketAcceptor() {
      @Override
      public Mono<Void> onConnect(WebSocketSession session) {
        return onConnect != null ? onConnect.apply(session) : Mono.empty();
      }

      @Override
      public Mono<Void> onDisconnect(WebSocketSession session) {
        return onDisconnect != null ? onDisconnect.apply(session) : Mono.empty();
      }
    });
    serverAddress = server.start();
    return serverAddress;
  }

  public <T> Flux<T> newClientSession(String path, List<T> requests, Class<T> clazz) {
    TestWebsocketClient client = new TestWebsocketClient(serverAddress, path);
    return client.send(requests, clazz);
  }

  @Override
  protected void after() {
    if (server != null) {
      server.stop();
    }
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop();
    }
  }
}
