package io.scalecube.services.gateway;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public interface GatewaySessionHandler {

  Logger LOGGER = System.getLogger(GatewaySessionHandler.class.getName());

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
    LOGGER.log(
        Level.ERROR,
        "Exception occurred on session: {0,number,#}, on context: {1}",
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
    LOGGER.log(
        Level.ERROR, "Exception occurred on session: {0,number,#}", session.sessionId(), throwable);
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
            LOGGER.log(
                Level.DEBUG,
                "Connection opened, sessionId: {0,number,#}, headers({1})",
                sessionId,
                headers.size()));
  }

  /**
   * On session open handler.
   *
   * @param session websocket session (not null)
   */
  default void onSessionOpen(GatewaySession session) {
    LOGGER.log(Level.INFO, "Session opened: {0}", session);
  }

  /**
   * On session close handler.
   *
   * @param session websocket session (not null)
   */
  default void onSessionClose(GatewaySession session) {
    LOGGER.log(Level.INFO, "Session closed: {0}", session);
  }
}
