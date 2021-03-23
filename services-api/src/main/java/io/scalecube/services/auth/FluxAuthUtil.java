package io.scalecube.services.auth;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class FluxAuthUtil {

  private FluxAuthUtil() {
    // Do not instantiate
  }

  public static <T> Flux<T> deferWithPrincipal(Class<T> clazz) {
    return Flux.deferContextual(context -> Mono.just(context.get(AUTH_CONTEXT_KEY))).cast(clazz);
  }
}
