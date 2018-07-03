package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

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

import java.util.Optional;

public final class WebSocketSession {

  public static final String DEFAULT_CONTENT_TYPE = "application/json";
  public static final int STATUS_CODE_NORMAL_CLOSE = 1000;

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;

  private final String id;
  private final String contentType;

  /**
   * Create a new websocket session with given handshake, inbound and outbound channels.
   * 
   * @param httpRequest - Init session HTTP request
   * @param inbound - Websocket inbound
   * @param outbound - Websocket outbound
   */
  public WebSocketSession(HttpServerRequest httpRequest, WebsocketInbound inbound, WebsocketOutbound outbound) {
    this.id = Integer.toHexString(System.identityHashCode(this));

    HttpHeaders httpHeaders = httpRequest.requestHeaders();
    this.contentType = Optional.ofNullable(httpHeaders.get(CONTENT_TYPE)).orElse(DEFAULT_CONTENT_TYPE);

    this.inbound = inbound;
    this.outbound = (WebsocketOutbound) outbound.options(NettyPipeline.SendOptions::flushOnEach);
  }

  public String id() {
    return id;
  }

  public String contentType() {
    return contentType;
  }

  public Flux<WebSocketFrame> receive() {
    return inbound.aggregateFrames()
        .receiveFrames()
        .map(WebSocketFrame::retain)
        .log(">> RECEIVE");
  }

  public Mono<Void> send(Publisher<ByteBuf> publisher) {
    return outbound.sendObject(
        Flux.from(publisher).map(TextWebSocketFrame::new).log("<< SEND")).then();
  }

  /**
   * Close the websocket session with <i>normal</i> status.
   * <a href="https://tools.ietf.org/html/rfc6455#section-7.4.1">Defined Status Codes:</a> <i>1000 indicates a normal
   * closure, meaning that the purpose for which the connection was established has been fulfilled.</i>
   */
  public Mono<Void> close() {
    return outbound.sendObject(new CloseWebSocketFrame(STATUS_CODE_NORMAL_CLOSE, "close")).then().log("<< CLOSE");
  }

  public void onClose(Runnable runnable) {
    inbound.context().onClose(runnable);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WebSocketSession{");
    sb.append("inbound=").append(inbound);
    sb.append(", outbound=").append(outbound);
    sb.append(", id='").append(id).append('\'');
    sb.append(", contentType='").append(contentType).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
