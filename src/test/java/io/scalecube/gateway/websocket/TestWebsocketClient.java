package io.scalecube.gateway.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

import io.netty.buffer.ByteBufAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

public class TestWebsocketClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestWebsocketClient.class);
  private final InetSocketAddress serverAddress;
  private final URI uri;
  private final ReactorNettyWebSocketClient client;
  private final NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  public TestWebsocketClient(InetSocketAddress serverAddress, String path) {
    this.serverAddress = serverAddress;
    this.uri = getUri(path);
    client = new ReactorNettyWebSocketClient();
  }

  public <T> Flux<T> send(List<T> requests, Class<T> clazz) {
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

  private <T> T decode(WebSocketMessage message, Class<T> clazz) {
    ByteBuffer buffer = message.getPayload().asByteBuffer();
    try {
      return new ObjectMapper().readValue(new ByteBufferBackedInputStream(buffer), clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
