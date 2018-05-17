package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.json.JsonObjectDecoder;

import org.reactivestreams.Publisher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public final class WebSocketSession {

  public static final String DEFAULT_CONTENT_TYPE = "application/json";
  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;

  private final String id;
  private final String uri;
  private final Map<String, String> headers;
  private final InetSocketAddress remoteAddress;
  private final String contentType;
  private final String auth;

  private static ObjectMapper mapper = new ObjectMapper();
  {
    mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(Visibility.ANY)
        .withGetterVisibility(Visibility.NONE)
        .withSetterVisibility(Visibility.NONE)
        .withCreatorVisibility(Visibility.NONE));
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  }

  public WebSocketSession(HttpServerRequest httpRequest,
      WebsocketInbound inbound,
      WebsocketOutbound outbound) {
    this.inbound = inbound;
    this.outbound = outbound;

    this.id = Integer.toHexString(System.identityHashCode(this));
    this.uri = httpRequest.uri();
    this.remoteAddress = httpRequest.remoteAddress();

    Map<String, String> headers = new HashMap<>();
    HttpHeaders httpHeaders = httpRequest.requestHeaders();
    httpHeaders.names().forEach(name -> {
      String value = httpHeaders.get(name);
      if (value != null) {
        headers.put(name, value);
      }
    });

    this.headers = Collections.unmodifiableMap(headers);

    this.contentType = Optional.ofNullable(httpHeaders.get(CONTENT_TYPE)).orElse(DEFAULT_CONTENT_TYPE);
    this.auth = httpHeaders.get(AUTHORIZATION);
  }

  public String id() {
    return id;
  }

  public String uri() {
    return uri;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public InetSocketAddress remoteAddress() {
    return remoteAddress;
  }

  public String contentType() {
    return contentType;
  }

  public Optional<String> auth() {
    return Optional.ofNullable(auth);
  }

  public Flux<ServiceMessage> receive() {
    if (uri.equals("/")) {
      return inbound
          .aggregateFrames()
          .receiveFrames()
          .map((WebSocketFrame frame) -> {
            ByteBuf content = frame.content();
            byte[] bytes = new byte[content.readableBytes()];
            int readerIndex = content.readerIndex();
            content.getBytes(readerIndex, bytes);
            try {
              JsonNode node = mapper.readValue(bytes, JsonNode.class);
              return ServiceMessage.builder()
                  .dataFormat(contentType)
                  .qualifier(node.get("q").asText())
                  .data(Unpooled.copiedBuffer(mapper.writeValueAsBytes(node.get("d"))))
                  .build();
            } catch (Exception e) {
              return ServiceMessage.builder().qualifier("error").build();
            } finally {
              frame.retain();
            }
          }).log();
    } else {
      return inbound
          .aggregateFrames()
          .receiveFrames()
          .map((WebSocketFrame frame) -> {
            ByteBuf content = frame.content();
            ServiceMessage message =
                ServiceMessage.builder().qualifier(uri.replaceFirst("/", "")).dataFormat(contentType).data(content)
                    .build();
            frame.retain();
            return message;
          }).log();
    }
  }

  public Mono<Void> send(Publisher<ServiceMessage> messages) {
    return outbound
        .options(NettyPipeline.SendOptions::flushOnEach)
        .sendObject(Flux
            .from(messages)
            .map(message -> (ByteBuf) message.data())
            .map(TextWebSocketFrame::new).log())
        .then();
  }

  /**
   * Close the websocket session with <i>normal</i> status.
   * <a href="https://tools.ietf.org/html/rfc6455#section-7.4.1">Defined Status Codes:</a> <i>1000 indicates a normal
   * closure, meaning that the purpose for which the connection was established has been fulfilled.</i>
   */
  public Mono<Void> close() {
    return outbound
        .options(NettyPipeline.SendOptions::flushOnEach)
        .sendObject(new CloseWebSocketFrame(1000, "close"))
        .then();
  }

  @Override
  public String toString() {
    return "WebSocketSession{" +
        "id='" + id + '\'' +
        ", uri='" + uri + '\'' +
        ", headers=" + headers +
        ", remoteAddress=" + remoteAddress +
        ", contentType='" + contentType + '\'' +
        ", auth='" + auth + '\'' +
        '}';
  }
}
