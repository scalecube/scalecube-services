package io.scalecube.gateway;

import static io.scalecube.gateway.core.GatewayMessage.DATA_FIELD;
import static io.scalecube.gateway.core.GatewayMessage.INACTIVITY_FIELD;
import static io.scalecube.gateway.core.GatewayMessage.QUALIFIER_FIELD;
import static io.scalecube.gateway.core.GatewayMessage.SIGNAL_FIELD;
import static io.scalecube.gateway.core.GatewayMessage.STREAM_ID_FIELD;

import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.websocket.WebsocketServer;
import io.scalecube.services.Microservices;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import reactor.core.publisher.Flux;

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
import java.util.HashMap;
import java.util.Map;

public class WebsocketResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketResource.class);

  private static final NettyDataBufferFactory BUFFER_FACTORY =
      new NettyDataBufferFactory(ByteBufAllocator.DEFAULT);

  private static final ObjectMapper objectMapper;
  static {
    objectMapper = initMapper();
  }

  private WebsocketServer websocketServer;
  private InetSocketAddress websocketServerAddress;
  private URI websocketServerUri;

  public WebsocketServer getWebsocketServer() {
    return websocketServer;
  }

  public InetSocketAddress getWebsocketServerAddress() {
    return websocketServerAddress;
  }

  public URI getWebsocketServerUri() {
    return websocketServerUri;
  }

  public WebsocketResource startWebsocketServer(Microservices gateway) {
    websocketServer = new WebsocketServer(gateway);
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

  public WebsocketResource stopWebsocketServer() {
    if (websocketServer != null) {
      try {
        websocketServer.stop();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Stopped websocket server {} on {}", websocketServer, websocketServerAddress);
    }
    return this;
  }

  public Flux<GatewayMessage> sendMessages(Publisher<GatewayMessage> messages, Duration timeout,
      Class<?>... dataClasses) {
    return sendPayloads(Flux.from(messages).map(this::encode), timeout, dataClasses);
  }

  public Flux<GatewayMessage> sendPayloads(Publisher<String> messages, Duration timeout, Class<?>... dataClasses) {
    return sendWebsocketMessages(Flux.from(messages).map(str -> {
      ByteBuf byteBuf = Unpooled.copiedBuffer(str, Charset.defaultCharset());
      return new WebSocketMessage(WebSocketMessage.Type.BINARY, BUFFER_FACTORY.wrap(byteBuf));
    }), timeout, dataClasses);
  }

  public Flux<GatewayMessage> sendWebsocketMessages(Publisher<WebSocketMessage> messages,
      Duration timeout,
      Class<?>... dataClasses) {
    ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    LOGGER.info("Created websocket client: {} for uri: {}", client, websocketServerUri);

    return Flux.create(emitter -> client
        .execute(websocketServerUri, session -> {
          LOGGER.info("{} started sending messages to: {}", client, websocketServerUri);
          return session.send(messages)
              .thenMany(session.receive()
                  .map(message -> decode(message.getPayloadAsText(), dataClasses))
                  .doOnNext(emitter::next)
                  .doOnComplete(emitter::complete)
                  .doOnError(emitter::error))
              .then();
        }).block(timeout));
  }

  private GatewayMessage decode(String payload, Class<?>[] dataClasses) {
    try {
      // noinspection unchecked
      Map<String, Object> map = objectMapper.readValue(payload, HashMap.class);
      Object data = map.get(DATA_FIELD);
      GatewayMessage.Builder builder = GatewayMessage.builder()
          .qualifier((String) map.get(QUALIFIER_FIELD))
          .streamId(map.containsKey(STREAM_ID_FIELD) ? Long.valueOf(String.valueOf(map.get(STREAM_ID_FIELD))) : null)
          .signal((Integer) map.get(SIGNAL_FIELD))
          .inactivity((Integer) map.get(INACTIVITY_FIELD));
      if (data != null) {
        Object content = data;
        for (Class<?> dataClass : dataClasses) {
          try {
            content = objectMapper.convertValue(data, dataClass);
            break;
          } catch (Exception e) {
            LOGGER.warn("Failed to decode data into {}: {}", dataClass, data);
          }
        }
        builder.data(content);
      }
      return builder.build();
    } catch (IOException e) {
      LOGGER.error("Failed to decode websocket message: " + payload);
      throw new RuntimeException(e);
    }
  }

  private String encode(GatewayMessage message) {
    try {
      Map<String, Object> response = new HashMap<>();
      response.put(QUALIFIER_FIELD, message.qualifier());
      response.put(STREAM_ID_FIELD, message.streamId());
      response.put(SIGNAL_FIELD, message.signal());
      response.put(INACTIVITY_FIELD, message.inactivity());
      response.put(DATA_FIELD, message.data());
      StringWriter writer = new StringWriter();
      objectMapper.writeValue(writer, response);
      writer.flush();
      return writer.getBuffer().toString();
    } catch (IOException e) {
      LOGGER.error("Failed to encode to websocket message: " + message);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void after() {
    stopWebsocketServer();
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
