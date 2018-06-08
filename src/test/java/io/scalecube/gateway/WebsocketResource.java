package io.scalecube.gateway;

import io.scalecube.gateway.websocket.WebSocketServer;
import io.scalecube.services.Microservices;
import io.scalecube.services.api.ServiceMessage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.junit.rules.ExternalResource;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;

public class WebsocketResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketResource.class);

  private static final NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  private static final ObjectMapper objectMapper;
  static {
    objectMapper = initMapper();
  }

  private WebSocketServer websocketServer;
  private InetSocketAddress websocketServerAddress;
  private URI websocketServerUri;

  public WebSocketServer getWebsocketServer() {
    return websocketServer;
  }

  public InetSocketAddress getWebsocketServerAddress() {
    return websocketServerAddress;
  }

  public URI getWebsocketServerUri() {
    return websocketServerUri;
  }

  public WebsocketResource startWebSocketServer(Microservices gateway) {
    websocketServer = new WebSocketServer(gateway);
    websocketServerAddress = websocketServer.start();

    String hostAddress = websocketServerAddress.getAddress().getHostAddress();
    int port = websocketServerAddress.getPort();
    websocketServerUri = UriComponentsBuilder.newInstance()
        .scheme("ws")
        .host(hostAddress)
        .port(port)
        .build().toUri();

    return this;
  }

  public WebsocketResource stopWebSocketServer() {
    if (websocketServer != null) {
      try {
        websocketServer.stop();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Stopped websocket server {} on {}", websocketServer, websocketServerAddress);
    }
    return this;
  }

  public Flux<ServiceMessage> sendMessages(Publisher<ServiceMessage> messages, Class<?> dataClass, Duration timeout) {
    return sendPayloads(Flux.from(messages).map(this::encode), dataClass, timeout);
  }

  public Flux<ServiceMessage> sendPayloads(Publisher<String> messages, Class<?> dataClass, Duration timeout) {
    return sendWebsocketMessages(Flux.from(messages).map(str -> {
      ByteBuf byteBuf = Unpooled.copiedBuffer(str, Charset.defaultCharset());
      return new WebSocketMessage(WebSocketMessage.Type.BINARY, BUFFER_FACTORY.wrap(byteBuf));
    }), dataClass, timeout);
  }

  public Flux<ServiceMessage> sendWebsocketMessages(Publisher<WebSocketMessage> messages,
      Class<?> dataClass,
      Duration timeout) {
    ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    LOGGER.info("Created websocket client: {} for uri: {}", client, websocketServerUri);

    return Flux.create((FluxSink<ServiceMessage> emitter) -> client
        .execute(websocketServerUri, session -> {
          LOGGER.info("{} started sending messages to: {}", client, websocketServerUri);
          return session.send(messages)
              .thenMany(
                  session.receive().map(message -> decode(message.getPayloadAsText(), dataClass))
                      .doOnNext(emitter::next)
                      .doOnComplete(emitter::complete)
                      .doOnError(emitter::error))
              .then();
        }).block(timeout));
  }

  private ServiceMessage decode(String payload, Class<?> dataClass) {
    try {
      ServiceMessage message = objectMapper.readValue(payload, ServiceMessage.class);
      if (message.hasData(Map.class)) {
        Object data = objectMapper.convertValue(message.<Map>data(), dataClass);
        return ServiceMessage.from(message).data(data).build();
      }
      return message;
    } catch (IOException e) {
      LOGGER.error("Failed to decode websocket message: " + payload);
      throw new RuntimeException(e);
    }
  }

  private String encode(Object message) {
    try {
      StringWriter writer = new StringWriter();
      objectMapper.writeValue(writer, message);
      writer.flush();
      return writer.getBuffer().toString();
    } catch (IOException e) {
      LOGGER.error("Failed to encode to websocket message: " + message);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void after() {
    stopWebSocketServer();
  }

  private static ObjectMapper initMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
