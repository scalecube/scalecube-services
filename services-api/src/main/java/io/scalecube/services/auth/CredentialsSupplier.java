package io.scalecube.services.auth;

import reactor.core.publisher.Mono;

/**
 * Supplier of credentials for authentication with remote service. Implementations can use {@code
 * byte[0]} to denote empty credentials.
 */
@FunctionalInterface
public interface CredentialsSupplier {

  /**
   * Obtains credentials for the given service role.
   *
   * @param service logical service name
   * @param serviceRole service role (optional)
   * @return credentials
   */
  Mono<byte[]> credentials(String service, String serviceRole);
}
