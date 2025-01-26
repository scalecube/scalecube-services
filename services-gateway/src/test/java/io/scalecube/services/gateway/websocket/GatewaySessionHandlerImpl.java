package io.scalecube.services.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.gateway.AuthRegistry;
import io.scalecube.services.gateway.GatewaySession;
import io.scalecube.services.gateway.GatewaySessionHandler;
import java.util.Optional;
import reactor.util.context.Context;

public class GatewaySessionHandlerImpl implements GatewaySessionHandler {

  private final AuthRegistry authRegistry;

  public GatewaySessionHandlerImpl(AuthRegistry authRegistry) {
    this.authRegistry = authRegistry;
  }

  @Override
  public Context onRequest(GatewaySession session, ByteBuf byteBuf, Context context) {
    Optional<String> authData = authRegistry.getAuth(session.sessionId());
    return authData.map(s -> context.put(Authenticator.AUTH_CONTEXT_KEY, s)).orElse(context);
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
