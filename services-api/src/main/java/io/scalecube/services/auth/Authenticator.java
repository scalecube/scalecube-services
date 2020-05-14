package io.scalecube.services.auth;

import java.util.Map;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface Authenticator<A> {

  /**
   * Key in {@link reactor.util.context.Context} to represent authentication result after call to
   * {@link #authenticate(Map)}.
   */
  String AUTH_CONTEXT_KEY = "auth.context";

  /**
   * Returns {@code authData} by given credentials.
   *
   * @param credentials credentials
   * @return async result with obtained {@code authData}
   */
  Mono<A> authenticate(Map<String, String> credentials);
}
