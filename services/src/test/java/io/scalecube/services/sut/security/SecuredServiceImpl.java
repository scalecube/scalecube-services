package io.scalecube.services.sut.security;

import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return deferSecured()
        .flatMap(
            principal -> {
              checkPrincipal(principal);
              return Mono.just("Hello | " + System.currentTimeMillis());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return deferSecured()
        .flatMap(
            principal -> {
              checkPrincipal(principal);
              return Mono.just("Hello, " + name);
            });
  }

  @Override
  public Mono<String> helloWithRoles(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPermissions(String name) {
    return Mono.just("Hello, " + name);
  }

  private static void checkPrincipal(Principal principal) {
    if (!"ADMIN".equals(principal.role())) {
      throw new ForbiddenException("Forbidden: wrong role=" + principal.role());
    }
  }
}
