package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public final class WebsocketSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketSession.class);

  public static final String DEFAULT_CONTENT_TYPE = "application/json";
  public static final int STATUS_CODE_NORMAL_CLOSE = 1000;

  private final Map<Long, Disposable> subscriptions = new ConcurrentHashMap<>();

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;
  private final GatewayMessageCodec messageCodec;

  private final String id;
  private final String contentType;

  /**
   * Create a new websocket session with given handshake, inbound and outbound channels.
   *
   * @param messageCodec - msg codec
   * @param httpRequest - Init session HTTP request
   * @param inbound - Websocket inbound
   * @param outbound - Websocket outbound
   */
  public WebsocketSession(
      GatewayMessageCodec messageCodec,
      HttpServerRequest httpRequest,
      WebsocketInbound inbound,
      WebsocketOutbound outbound) {
    this.messageCodec = messageCodec;
    this.id = Integer.toHexString(System.identityHashCode(this));

    HttpHeaders httpHeaders = httpRequest.requestHeaders();
    this.contentType =
        Optional.ofNullable(httpHeaders.get(CONTENT_TYPE)).orElse(DEFAULT_CONTENT_TYPE);

    this.inbound = inbound;
    this.outbound = (WebsocketOutbound) outbound.options(SendOptions::flushOnEach);

    inbound.context().onClose(this::clearSubscriptions);
  }

  public String id() {
    return id;
  }

  public String contentType() {
    return contentType;
  }

  /**
   * Method for receiving request messages coming a form of websocket frames.
   *
   * @return flux websocket {@link ByteBuf}
   */
  public Flux<ByteBuf> receive() {
    return inbound.aggregateFrames().receive().retain().log(">> RECEIVE", Level.FINE);
  }

  /**
   * Method to send normal reply.
   *
   * @param response response
   * @return mono void
   */
  public Mono<Void> send(GatewayMessage response) {
    return send(Mono.just(response).map(messageCodec::encode));
  }

  /**
   * Method to send error reply.
   *
   * @param throwable error
   * @param sid stream id; optional
   * @return mono void
   */
  public Mono<Void> send(Throwable throwable, Long sid) {
    return send(
        Mono.just(throwable)
            .map(ExceptionProcessor::toMessage)
            .map(msg -> GatewayMessage.from(msg).streamId(sid).signal(Signal.ERROR).build())
            .map(messageCodec::encode));
  }

  private Mono<Void> send(Publisher<ByteBuf> publisher) {
    return outbound
        .sendObject(Flux.from(publisher).map(TextWebSocketFrame::new).log("<< SEND", Level.FINE))
        .then();
  }

  /**
   * Close the websocket session with <i>normal</i> status. <a
   * href="https://tools.ietf.org/html/rfc6455#section-7.4.1">Defined Status Codes:</a> <i>1000
   * indicates a normal closure, meaning that the purpose for which the connection was established
   * has been fulfilled.</i>
   *
   * @return mono void
   */
  public Mono<Void> close() {
    return outbound
        .sendObject(new CloseWebSocketFrame(STATUS_CODE_NORMAL_CLOSE, "close"))
        .then()
        .log("<< CLOSE", Level.FINE);
  }

  /**
   * Lambda setter for reacting on channel close occurence.
   *
   * @param runnable lambda
   */
  public Mono<Void> onClose(Runnable runnable) {
    return inbound.context().onClose(runnable).onClose();
  }

  /**
   * Disposing stored subscription by given stream id.
   *
   * @param streamId stream id
   * @return true of subscription was disposed
   */
  public boolean dispose(Long streamId) {
    boolean result = false;
    if (streamId != null) {
      Disposable disposable = subscriptions.remove(streamId);
      result = disposable != null;
      if (result) {
        LOGGER.debug("Dispose subscription by sid: {} on session: {}", streamId, this);
        disposable.dispose();
      }
    }
    return result;
  }

  public boolean containsSid(Long streamId) {
    return streamId != null && subscriptions.containsKey(streamId);
  }

  /**
   * Saves (if not already saved) by stream id a subscrption of service call coming in form of
   * {@link Disposable} reference.
   *
   * @param streamId stream id
   * @param disposable service subscrption
   * @return true if disposable subscrption was stored
   */
  public boolean register(Long streamId, Disposable disposable) {
    boolean result = false;
    if (!disposable.isDisposed()) {
      result = subscriptions.putIfAbsent(streamId, disposable) == null;
    }
    if (result) {
      LOGGER.debug("Registered subscription with sid: {} on session: {}", streamId, this);
    }
    return result;
  }

  private void clearSubscriptions() {
    if (!subscriptions.isEmpty()) {
      LOGGER.info("Clear all {} subscriptions on session: {}", subscriptions.size(), this);
    }
    subscriptions.forEach((sid, disposable) -> disposable.dispose());
    subscriptions.clear();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WebsocketSession{");
    sb.append("id='").append(id).append('\'');
    sb.append(", contentType='").append(contentType).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
