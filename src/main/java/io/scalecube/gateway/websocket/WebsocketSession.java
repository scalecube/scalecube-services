package io.scalecube.gateway.websocket;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
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

  private final String id;
  private final String contentType;

  /**
   * Create a new websocket session with given handshake, inbound and outbound channels.
   *
   * @param httpRequest - Init session HTTP request
   * @param inbound - Websocket inbound
   * @param outbound - Websocket outbound
   */
  public WebsocketSession(
      HttpServerRequest httpRequest, WebsocketInbound inbound, WebsocketOutbound outbound) {
    this.id = Integer.toHexString(System.identityHashCode(this));

    HttpHeaders httpHeaders = httpRequest.requestHeaders();
    this.contentType =
        Optional.ofNullable(httpHeaders.get(CONTENT_TYPE)).orElse(DEFAULT_CONTENT_TYPE);

    this.inbound = inbound;
    this.outbound = (WebsocketOutbound) outbound.options(NettyPipeline.SendOptions::flushOnEach);

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
   * @return flux websocket frame
   */
  public Flux<WebSocketFrame> receive() {
    return inbound.aggregateFrames().receiveFrames().map(WebSocketFrame::retain).log(">> RECEIVE");
  }

  /**
   * Method for send replies which taken in a form of publisher of byte buffers.
   *
   * @param publisher byte buf publisher
   * @return mono void
   */
  public Mono<Void> send(Publisher<ByteBuf> publisher) {
    return outbound
        .sendObject(Flux.from(publisher).map(BinaryWebSocketFrame::new).log("<< SEND"))
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
        .log("<< CLOSE");
  }

  /**
   * Lambda setter for reacting on channel close occurence.
   *
   * @param runnable lambda
   */
  public void onClose(Runnable runnable) {
    inbound.context().onClose(runnable);
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
        LOGGER.debug("Dispose subscription by streamId: {} on session: {}", streamId, this);
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
   * @param serviceSubscription service subscrption
   * @return true if disposable subscrption was stored
   */
  public boolean register(Long streamId, Disposable serviceSubscription) {
    boolean result = subscriptions.putIfAbsent(streamId, serviceSubscription) == null;
    if (result) {
      LOGGER.debug("Registered subscrption with streamId: {} on session: {}", streamId, this);
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
