package io.scalecube.services.auth;

import java.util.List;
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
   * @param serviceRoles allowed roles on the service (optional)
   * @return credentials
   */
  Mono<byte[]> credentials(String service, List<String> serviceRoles);
}
