package io.scalecube.gateway.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import io.netty.buffer.ByteBufAllocator;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class WebSocketResource extends ExternalResource implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketResource.class);

  private static NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  private WebSocketServer server;
  private InetSocketAddress serverAddress;
  private WebSocketClient client;

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
    client = new ReactorNettyWebSocketClient();
    URI uri = getUri(path);
    ReplayProcessor<T> responses = ReplayProcessor.create();

    client.execute(uri,
        session -> {
          LOGGER.info("Start sending messages");

          return session
              .send(Flux.fromIterable(requests).map(this::encode))
              .thenMany(session.receive()
                  .map(nxt -> this.decode(nxt, clazz))
                  .take(requests.size())
                  .subscribeWith(responses))
              .then();

        })
        .doOnSuccessOrError((aVoid, ex) -> LOGGER.debug("Done: " + (ex != null ? ex.getMessage() : "success")))
        .block(Duration.ofSeconds(10));
    return responses;
  }

  private URI getUri(String path) {
    UriComponentsBuilder builder = UriComponentsBuilder
        .newInstance()
        .scheme("ws")
        .host(serverAddress.getAddress().getHostAddress())
        .port(serverAddress.getPort());

    if (path != null) {
      builder.path(path);
    }

    return builder.build().toUri();
  }

  private <T> T decode(WebSocketMessage message, Class<T> clazz) {
    ByteBuffer buffer = message.getPayload().asByteBuffer();
    try {
      return new ObjectMapper().readValue(new ByteBufferBackedInputStream(buffer), clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private WebSocketMessage encode(Object message) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new ObjectMapper().writeValue(baos, message);
      NettyDataBuffer dataBuffer = BUFFER_FACTORY.allocateBuffer();
      dataBuffer.write(baos.toByteArray());
      return new WebSocketMessage(WebSocketMessage.Type.BINARY, dataBuffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
