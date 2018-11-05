package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessage.Builder;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.util.Map;
import java.util.Optional;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

public final class WebsocketSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketSession.class);

  private static final String DEFAULT_CONTENT_TYPE = "application/json";

  private final Map<Long, Disposable> subscriptions = new NonBlockingHashMapLong<>(1024);

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

    this.inbound =
        (WebsocketInbound)
            inbound.withConnection(connection -> connection.onDispose(this::clearSubscriptions));
    this.outbound = (WebsocketOutbound) outbound.options(SendOptions::flushOnEach);
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
    return inbound.aggregateFrames().receive().retain();
  }

  /**
   * Method to send normal response.
   *
   * @param response response
   * @return mono void
   */
  public Mono<Void> send(GatewayMessage response) {
    return send(Mono.just(response).map(messageCodec::encode))
        .doOnSuccessOrError((avoid, th) -> logSend(response, th));
  }

  /**
   * Method to send error response.
   *
   * @param err error
   * @param sid stream id; optional
   * @return mono void
   */
  public Mono<Void> send(Throwable err, Long sid) {
    return send(Mono.just(err)
            .map(ExceptionProcessor::toMessage)
            .map(msg -> toErrorMessage(sid, msg))
            .map(messageCodec::encode))
        .doOnSuccessOrError((avoid, th) -> logSend(err, sid, th));
  }

  private Mono<Void> send(Mono<ByteBuf> publisher) {
    return publisher
        .map(TextWebSocketFrame::new)
        .flatMap(frame -> outbound.sendObject(frame).then());
  }

  private void logSend(GatewayMessage response, Throwable th) {
    if (th == null) {
      LOGGER.debug("<< SEND success: {}, session={}", response, id);
    } else {
      LOGGER.warn("<< SEND failed: {}, session={}, cause: {}", response, id, th);
    }
  }

  private void logSend(Throwable err, Long sid, Throwable th) {
    if (th == null) {
      LOGGER.debug("<< SEND success: {}, sid={}, session={}", err, sid, id);
    } else {
      LOGGER.warn("<< SEND failed: {}, sid={}, session={}, cause: {}", err, sid, id, th);
    }
  }

  private GatewayMessage toErrorMessage(Long sid, ServiceMessage msg) {
    Builder builder = GatewayMessage.from(msg);
    Optional.ofNullable(sid).ifPresent(builder::streamId);
    return builder.signal(Signal.ERROR).build();
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
    return outbound.sendClose().then();
  }

  /**
   * Lambda setter for reacting on channel close occurence.
   *
   * @param disposable function to run when disposable would take place
   */
  public Mono<Void> onClose(Disposable disposable) {
    return Mono.create(
        sink ->
            inbound.withConnection(
                connection ->
                    connection
                        .onDispose(disposable)
                        .onTerminate()
                        .subscribe(sink::success, sink::error, sink::success)));
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
        LOGGER.debug("Dispose subscription by sid={}, session={}", streamId, id);
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
      LOGGER.debug("Registered subscription with sid={}, session={}", streamId, id);
    }
    return result;
  }

  private void clearSubscriptions() {
    if (subscriptions.size() > 1) {
      LOGGER.info("Clear all {} subscriptions on session={}", subscriptions.size(), id);
    } else if (subscriptions.size() == 1) {
      LOGGER.info("Clear 1 subscription on session={}", id);
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
