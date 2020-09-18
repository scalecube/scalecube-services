package io.scalecube.services.examples.auth;

import io.scalecube.services.auth.MonoAuthUtil;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceByApiKeyImpl implements SecuredServiceByApiKey {

  @Override
  public Mono<String> hello(String name) {
    return MonoAuthUtil.deferWithPrincipal(ApiKey.class)
        .flatMap(
            apiKey -> {
              checkPermissions(apiKey);
              return Mono.just("Hello, name=" + name + " (apiKey=" + apiKey + ")");
            });
  }

  private void checkPermissions(ApiKey apiKey) {
    if (!apiKey.permissions().equals("OPERATIONS:EVENTS:ACTIONS")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
