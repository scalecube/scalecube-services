package io.scalecube.services.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.GatewaySession;
import io.scalecube.services.gateway.GatewaySessionHandler;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.jctools.maps.NonBlockingHashMapLong;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;
import reactor.util.context.Context;

public final class WebsocketGatewaySession implements GatewaySession {

  private static final Logger LOGGER = System.getLogger(WebsocketGatewaySession.class.getName());

  private static final Predicate<Object> SEND_PREDICATE = f -> true;

  private final Map<Long, Disposable> subscriptions = new NonBlockingHashMapLong<>(1024);

  private final GatewaySessionHandler gatewayHandler;

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;
  private final WebsocketServiceMessageCodec codec;

  private final long sessionId;
  private final Map<String, String> headers;

  /**
   * Create a new websocket session with given handshake, inbound and outbound channels.
   *
   * @param sessionId - session id
   * @param codec - msg codec
   * @param headers - headers
   * @param inbound - Websocket inbound
   * @param outbound - Websocket outbound
   * @param gatewayHandler - gateway handler
   */
  public WebsocketGatewaySession(
      long sessionId,
      WebsocketServiceMessageCodec codec,
      Map<String, String> headers,
      WebsocketInbound inbound,
      WebsocketOutbound outbound,
      GatewaySessionHandler gatewayHandler) {
    this.sessionId = sessionId;
    this.codec = codec;

    this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
    this.inbound =
        (WebsocketInbound) inbound.withConnection(c -> c.onDispose(this::clearSubscriptions));
    this.outbound = outbound;
    this.gatewayHandler = gatewayHandler;
  }

  @Override
  public long sessionId() {
    return sessionId;
  }

  @Override
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Method for receiving request messages coming a form of websocket frames.
   *
   * @return flux websocket {@link ByteBuf}
   */
  public Flux<ByteBuf> receive() {
    return inbound.receive().retain();
  }

  /**
   * Method to send normal response.
   *
   * @param response response
   * @return mono void
   */
  public Mono<Void> send(ServiceMessage response) {
    return Mono.deferContextual(
        context -> {
          final TextWebSocketFrame frame = new TextWebSocketFrame(codec.encode(response));
          gatewayHandler.onResponse(this, frame.content(), response, (Context) context);
          // send with publisher (defer buffer cleanup to netty)
          return outbound
              .sendObject(frame)
              .then()
              .doOnError(th -> gatewayHandler.onError(this, th, (Context) context));
        });
  }

  /**
   * Method to send normal response.
   *
   * @param messages messages
   * @return mono void
   */
  public Mono<Void> send(Flux<ServiceMessage> messages) {
    return Mono.deferContextual(
        context -> {
          // send with publisher (defer buffer cleanup to netty)
          return outbound
              .sendObject(
                  messages.map(
                      response -> {
                        final TextWebSocketFrame frame =
                            new TextWebSocketFrame(codec.encode(response));
                        gatewayHandler.onResponse(
                            this, frame.content(), response, (Context) context);
                        return frame;
                      }),
                  SEND_PREDICATE)
              .then()
              .doOnError(th -> gatewayHandler.onError(this, th, (Context) context));
        });
  }

  /**
   * Close the websocket session.
   *
   * @return mono void
   */
  public Mono<Void> close() {
    return outbound.sendClose().then();
  }

  /**
   * Closes websocket session with <i>normal</i> status.
   *
   * @param reason close reason
   * @return mono void
   */
  public Mono<Void> close(String reason) {
    return outbound.sendClose(1000, reason).then();
  }

  /**
   * Lambda setter for reacting on channel close occurrence.
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
        LOGGER.log(
            Level.DEBUG,
            "Dispose subscription by sid={0,number,#}, session={1,number,#}",
            streamId,
            sessionId);
        disposable.dispose();
      }
    }
    return result;
  }

  public boolean containsSid(Long streamId) {
    return streamId != null && subscriptions.containsKey(streamId);
  }

  /**
   * Saves (if not already saved) by stream id a subscription of service call coming in form of
   * {@link Disposable} reference.
   *
   * @param streamId stream id
   * @param disposable service subscription
   */
  public void register(Long streamId, Disposable disposable) {
    boolean result = false;
    if (!disposable.isDisposed()) {
      result = subscriptions.putIfAbsent(streamId, disposable) == null;
    }
    if (result) {
      LOGGER.log(
          Level.DEBUG,
          "Registered subscription by sid={0,number,#}, session={1,number,#}",
          streamId,
          sessionId);
    }
  }

  private void clearSubscriptions() {
    LOGGER.log(Level.DEBUG, "Clear subscriptions on session={0,number,#}", sessionId);
    subscriptions.forEach((sid, disposable) -> disposable.dispose());
    subscriptions.clear();
  }

  @Override
  public String toString() {
    return "WebsocketGatewaySession[" + sessionId + ']';
  }
}
