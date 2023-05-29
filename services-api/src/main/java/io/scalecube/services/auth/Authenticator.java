package io.scalecube.services.auth;

import java.util.Map;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 * Returns auth data by given credentials. Client code shall store returned result under {@link
 * Authenticator#AUTH_CONTEXT_KEY} key in {@link reactor.util.context.Context} to propagate auth
 * data to downstream components.
 *
 * @see PrincipalMapper
 * @param <R> auth data type
 */
@FunctionalInterface
public interface Authenticator<R> extends Function<Map<String, String>, Mono<R>> {

  Object NULL_AUTH_CONTEXT = new Object();

  String AUTH_CONTEXT_KEY = "auth.context";

  static <T> Mono<T> deferSecured(Class<T> authDataType) {
    return Mono.deferContextual(context -> Mono.just(context.get(AUTH_CONTEXT_KEY)))
        .cast(authDataType);
  }
}
