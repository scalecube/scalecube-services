package io.scalecube.services.gateway;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public interface GatewaySessionHandler {

  Logger LOGGER = LoggerFactory.getLogger(GatewaySessionHandler.class);

  GatewaySessionHandler DEFAULT_INSTANCE = new GatewaySessionHandler() {};

  /**
   * Message mapper function.
   *
   * @param session webscoket session (not null)
   * @param message request message (not null)
   * @return message
   */
  default ServiceMessage mapMessage(
      GatewaySession session, ServiceMessage message, Context context) {
    return message;
  }

  /**
   * Request mapper function.
   *
   * @param session session
   * @param byteBuf request buffer
   * @param context subscriber context
   * @return context
   */
  default Context onRequest(GatewaySession session, ByteBuf byteBuf, Context context) {
    return context;
  }

  /**
   * On response handler.
   *
   * @param session session
   * @param byteBuf response buffer
   * @param message response message
   * @param context subscriber context
   */
  default void onResponse(
      GatewaySession session, ByteBuf byteBuf, ServiceMessage message, Context context) {
    // no-op
  }

  /**
   * Error handler function.
   *
   * @param session webscoket session (not null)
   * @param throwable an exception that occurred (not null)
   * @param context subscriber context
   */
  default void onError(GatewaySession session, Throwable throwable, Context context) {
    LOGGER.error(
        "Exception occurred on session: {}, on context: {}, cause:",
        session.sessionId(),
        context,
        throwable);
  }

  /**
   * Error handler function.
   *
   * @param session webscoket session (not null)
   * @param throwable an exception that occurred (not null)
   */
  default void onSessionError(GatewaySession session, Throwable throwable) {
    LOGGER.error("Exception occurred on session: {}, cause:", session.sessionId(), throwable);
  }

  /**
   * On connection open handler.
   *
   * @param sessionId session id
   * @param headers connection/session headers
   * @return mono result
   */
  default Mono<Void> onConnectionOpen(long sessionId, Map<String, String> headers) {
    return Mono.fromRunnable(
        () ->
            LOGGER.debug(
                "Connection opened, sessionId: {}, headers({})", sessionId, headers.size()));
  }

  /**
   * On session open handler.
   *
   * @param session websocket session (not null)
   */
  default void onSessionOpen(GatewaySession session) {
    LOGGER.info("Session opened: {}", session);
  }

  /**
   * On session close handler.
   *
   * @param session websocket session (not null)
   */
  default void onSessionClose(GatewaySession session) {
    LOGGER.info("Session closed: {}", session);
  }
}
