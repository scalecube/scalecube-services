package io.scalecube.services.auth;

import io.scalecube.services.exceptions.UnauthorizedException;
import java.util.Map;
import reactor.core.publisher.Mono;

public final class DelegatingAuthenticator implements Authenticator<Object> {

  @Override
  public Mono<Object> authenticate(Map<String, String> credentials) {
    return Mono.deferWithContext(
        context -> {
          if (!context.hasKey(Authenticator.AUTH_CONTEXT_KEY)) {
            throw new UnauthorizedException("Authentication failed (auth context not found)");
          }
          return context.get(Authenticator.AUTH_CONTEXT_KEY);
        });
  }
}
