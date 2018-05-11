package io.scalecube.gateway.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import io.netty.buffer.ByteBufAllocator;

import org.junit.rules.ExternalResource;
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
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

public class WebSocketResource extends ExternalResource implements Closeable {

  private WebSocketServer server;
  private InetSocketAddress serverAddress;
  private WebSocketClient client;
  private NettyDataBufferFactory bufferFactory = new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);;

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

  public Flux<Object> newClientSession(String path, Flux<Object> flux) {
    client = new ReactorNettyWebSocketClient();

    return FluxProcessor.create(emitter -> {
      UriComponentsBuilder builder = UriComponentsBuilder
          .newInstance()
          .scheme("ws")
          .host(serverAddress.getAddress().getHostAddress())
          .port(serverAddress.getPort());

      if (path != null) {
        builder.path(path);
      }

      URI url = builder.build().toUri();

      client.execute(url, session -> session.send(flux.map(this::encode)).log()
          .thenMany(session.receive().map(this::decode).log())
          .then());
    });
  }

  private Object decode(WebSocketMessage message) {
    ByteBuffer buffer = message.getPayload().asByteBuffer();
    try {
      return new ObjectMapper().readValue(new ByteBufferBackedInputStream(buffer), Object.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private WebSocketMessage encode(Object message) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new ObjectMapper().writeValue(baos, message);
      NettyDataBuffer dataBuffer = bufferFactory.allocateBuffer();
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
