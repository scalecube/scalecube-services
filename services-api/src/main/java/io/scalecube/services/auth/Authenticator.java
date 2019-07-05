package io.scalecube.services.auth;

import reactor.core.publisher.Mono;

/**
 * Authenticator.
 *
 * @param <C> credentials type
 * @param <P> principal type
 */
public interface Authenticator<C, P> {

  /**
   * Returns principal by given credentials.
   *
   * @param credentials credentials
   * @return async result with obtained principal
   */
  Mono<P> authenticate(C credentials);
}
