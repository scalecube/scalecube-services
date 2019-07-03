package io.scalecube.services.auth;

import reactor.core.publisher.Mono;

/**
 * Authneticator.
 *
 * @param <C> credentials type
 * @param <P> principal type
 */
public interface Authenticator<C, P> {

  /**
   * Retrusn principal by given credenatitsl.
   *
   * @param credentials credentials
   * @return asycn result with obtained principal
   */
  Mono<P> authenticate(C credentials);
}
