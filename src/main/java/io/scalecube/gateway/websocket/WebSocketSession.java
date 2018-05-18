package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class WebSocketSession {

  public static final String DEFAULT_CONTENT_TYPE = "application/json";
  public static final int STATUS_CODE_NORMAL_CLOSE = 1000;

  private final Flux<ServiceMessage> inbound;
  private final WebsocketOutbound outbound;

  private final String id;
  private final String uri;
  private final Map<String, String> headers;
  private final InetSocketAddress remoteAddress;
  private final String contentType;
  private final String auth;

  /**
   * Create a new websocket session with given handshake, inbound and outbound channels.
   * 
   * @param httpRequest - Init session HTTP request
   * @param inbound - Websocket inbound
   * @param outbound - Websocket outbound
   */
  public WebSocketSession(HttpServerRequest httpRequest, WebsocketInbound inbound, WebsocketOutbound outbound) {
    this.id = Integer.toHexString(System.identityHashCode(this));
    this.uri = httpRequest.uri();
    this.remoteAddress = httpRequest.remoteAddress();

    // prepare headers
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

    // prepare inbound
    this.inbound = inbound.aggregateFrames().receiveFrames().map(this::toMessage).log("++++++++++ RECEIVE");

    // prepare outbound
    this.outbound = (WebsocketOutbound) outbound.options(NettyPipeline.SendOptions::flushOnEach);
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
    return inbound;
  }

  public Mono<Void> send(Publisher<ServiceMessage> messages) {
    return outbound.sendObject(Flux.from(messages).map(this::toFrame)).then();
  }

  /**
   * Close the websocket session with <i>normal</i> status.
   * <a href="https://tools.ietf.org/html/rfc6455#section-7.4.1">Defined Status Codes:</a> <i>1000 indicates a normal
   * closure, meaning that the purpose for which the connection was established has been fulfilled.</i>
   */
  public Mono<Void> close() {
    return outbound.sendObject(new CloseWebSocketFrame(STATUS_CODE_NORMAL_CLOSE, "close")).then();
  }

  private ServiceMessage toMessage(WebSocketFrame frame) {
    return ServiceMessage.builder()
        .qualifier(uri)
        .dataFormat(contentType)
        .data(frame.content().retain())
        .build();
  }

  private WebSocketFrame toFrame(ServiceMessage message) {
    return new TextWebSocketFrame(((ByteBuf) message.data()));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WebSocketSession{");
    sb.append("inbound=").append(inbound);
    sb.append(", outbound=").append(outbound);
    sb.append(", id='").append(id).append('\'');
    sb.append(", uri='").append(uri).append('\'');
    sb.append(", headers=").append(headers);
    sb.append(", remoteAddress=").append(remoteAddress);
    sb.append(", contentType='").append(contentType).append('\'');
    sb.append(", auth='").append(auth).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
