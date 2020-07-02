package io.scalecube.services.auth;

import io.scalecube.services.ServiceReference;
import java.util.Map;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface CredentialsSupplier {

  /**
   * Returns credentials for the given {@link ServiceReference}.
   *
   * @param serviceReference target serviceReference
   * @return mono result
   */
  Mono<Map<String, String>> getCredentials(ServiceReference serviceReference);
}
