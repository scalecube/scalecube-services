package io.scalecube.services.auth;

import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Mono;

/**
 * Authenticator.
 *
 * @param <P> principal type
 */
public interface Authenticator<P> {

  /**
   * Returns principal by given credentials.
   *
   * @param message service message
   * @param authContextRegistry auth context registry
   * @return async result with obtained principal
   */
  Mono<P> authenticate(ServiceMessage message, AuthContextRegistry authContextRegistry);
}
