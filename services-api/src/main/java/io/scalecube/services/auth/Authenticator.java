package io.scalecube.services.auth;

import java.util.Map;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface Authenticator {

  /**
   * Returns {@code authData} by given credentials.
   *
   * @param credentials credentials
   * @return async result with obtained {@code authData}
   */
  Mono<Map<String, String>> authenticate(Map<String, String> credentials);
}
