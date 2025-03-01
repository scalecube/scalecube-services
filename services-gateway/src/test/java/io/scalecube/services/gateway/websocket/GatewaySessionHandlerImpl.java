package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.AuthRegistry;
import io.scalecube.services.gateway.GatewaySession;
import io.scalecube.services.gateway.GatewaySessionHandler;
import reactor.util.context.Context;

public class GatewaySessionHandlerImpl implements GatewaySessionHandler {

  private final AuthRegistry authRegistry;

  public GatewaySessionHandlerImpl(AuthRegistry authRegistry) {
    this.authRegistry = authRegistry;
  }

  @Override
  public Context onRequest(GatewaySession session, ByteBuf byteBuf, Context context) {
    final var principal = authRegistry.getAuth(session.sessionId());
    return RequestContext.builder()
        .principal(principal != null ? principal : NULL_PRINCIPAL)
        .build()
        .toContext();
  }

  @Override
  public ServiceMessage mapMessage(
      GatewaySession session, ServiceMessage message, Context context) {
    return ServiceMessage.from(message)
        .header(AuthRegistry.SESSION_ID, session.sessionId())
        .build();
  }

  @Override
  public void onSessionClose(GatewaySession session) {
    authRegistry.removeAuth(session.sessionId());
    LOGGER.info("Session removed: {}", session);
  }
}
