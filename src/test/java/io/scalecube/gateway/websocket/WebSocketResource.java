package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.transport.Address;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
import java.time.Duration;
import java.util.Map;

public class WebSocketResource extends ExternalResource implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketResource.class);

  private static final NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  private static final ObjectMapper objectMapper;
  static {
    objectMapper = initMapper();
  }

  private Microservices gateway;
  private WebSocketServer server;
  private InetSocketAddress serverAddress;
  private Address gatewayAddress;
  private Microservices services;

  public InetSocketAddress serverAddress() {
    return serverAddress;
  }

  public Address gatewayAddress() {
    return gatewayAddress;
  }

  public WebSocketResource startServer() {
    gateway = Microservices.builder().build().startAwait();
    gatewayAddress = gateway.cluster().address();
    server = new WebSocketServer(gateway);
    serverAddress = server.start();
    return this;
  }

  public WebSocketResource startServices() {
    services = Microservices.builder()
        .seeds(gatewayAddress)
        .services(new GreetingServiceImpl())
        .build().startAwait();
    return this;
  }

  public Flux<ServiceMessage> sendThenReceive(Publisher<ServiceMessage> requests, Class<?> dataClass,
      Duration timeout) {
    String hostAddress = serverAddress.getAddress().getHostAddress();
    int port = serverAddress.getPort();
    URI uri = UriComponentsBuilder.newInstance().scheme("ws").host(hostAddress).port(port).build().toUri();

    ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    LOGGER.info("Created websocket client: {} for uri: {}", client, uri);

    return Flux.create((FluxSink<ServiceMessage> emitter) -> client
        .execute(uri, session -> {
          LOGGER.info("{} started sending messages to: {}", client, uri);
          return session.send(Flux.from(requests).map(this::encode))
              .thenMany(
                  session.receive().map(message -> decode(message.getPayloadAsText(), dataClass))
                      .doOnNext(emitter::next)
                      .doOnComplete(emitter::complete)
                      .doOnError(emitter::error))
              .then();
        }).block(timeout));
  }

  private ServiceMessage decode(String payload, Class<?> dataClass) {
    LOGGER.info("Decoding websocket message: " + payload);
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

  private WebSocketMessage encode(Object message) {
    LOGGER.info("Encoding to websocket message: " + message);
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      objectMapper.writeValue(baos, message);
      NettyDataBuffer dataBuffer = BUFFER_FACTORY.allocateBuffer();
      dataBuffer.write(baos.toByteArray());
      return new WebSocketMessage(WebSocketMessage.Type.BINARY, dataBuffer);
    } catch (IOException e) {
      LOGGER.error("Failed to encode to websocket message: " + message);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void after() {
    if (gateway != null) {
      gateway.shutdown();
    }
    if (server != null) {
      server.stop();
    }
    if (services != null) {
      services.shutdown();
    }
  }

  @Override
  public void close() {
    if (gateway != null) {
      gateway.shutdown();
    }
    if (server != null) {
      server.stop();
    }
    if (services != null) {
      services.shutdown();
    }
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
