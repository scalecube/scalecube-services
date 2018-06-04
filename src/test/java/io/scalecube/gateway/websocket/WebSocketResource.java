package io.scalecube.gateway.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import io.netty.buffer.ByteBufAllocator;

import org.junit.rules.ExternalResource;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class WebSocketResource extends ExternalResource implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketResource.class);

  private static NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  private WebSocketServer server;
  private InetSocketAddress serverAddress;

  public InetSocketAddress startServer(Function<WebSocketSession, Mono<Void>> onConnect,
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

    return serverAddress = server.start();
  }

  public <T> Flux<T> sendAndReceive(Publisher<?> requests, Class<T> clazz) {
    return EmitterProcessor.create((FluxSink<T> emitter) -> {
      URI uri = getUri();
      ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
      client
          .execute(uri, session -> {
            LOGGER.info("Start sending messages to: " + uri);
            return session.send(Flux.from(requests).map(this::encode))
                .thenMany(session
                    .receive()
                    .map(message -> decode(message, clazz))
                    .doOnNext(emitter::next)
                    .doOnComplete(emitter::complete)
                    .doOnError(emitter::error))
                .then();
          }).then().block();
    });
  }

  private URI getUri() {
    return UriComponentsBuilder
        .newInstance()
        .scheme("ws")
        .host(serverAddress.getAddress().getHostAddress())
        .port(serverAddress.getPort())
        .path("/")
        .build()
        .toUri();
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
