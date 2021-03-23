package io.scalecube.services.auth;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;

import reactor.core.publisher.Mono;

public final class MonoAuthUtil {

  private MonoAuthUtil() {
    // Do not instantiate
  }

  public static <T> Mono<T> deferWithPrincipal(Class<T> clazz) {
    return Mono.deferContextual(context -> Mono.just(context.get(AUTH_CONTEXT_KEY))).cast(clazz);
  }
}
