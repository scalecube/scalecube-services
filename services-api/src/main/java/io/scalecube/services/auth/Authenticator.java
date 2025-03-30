package io.scalecube.services.auth;

import reactor.core.publisher.Mono;

/**
 * Service authentication interface to handle authentication of clients to the service. Result of
 * authentication is abstract {@link Principal} with role and permissions.
 */
@FunctionalInterface
public interface Authenticator {

  /**
   * Authenticates service clients by given credentials.
   *
   * @param credentials credentials
   * @return result
   */
  Mono<Principal> authenticate(byte[] credentials);
}
