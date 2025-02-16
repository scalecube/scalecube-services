package io.scalecube.services.sut.security;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return Authenticator.deferSecured(Principal.class)
        .flatMap(
            principal -> {
              checkPrincipal(principal);
              return Mono.just("Hello | " + System.currentTimeMillis());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return Authenticator.deferSecured(Principal.class)
        .flatMap(
            principal -> {
              checkPrincipal(principal);
              return Mono.just("Hello, " + name);
            });
  }

  @Override
  public Mono<String> helloWithRoles() {
    return null; // TODO
  }

  @Override
  public Mono<String> helloWithPermissions() {
    return null; // TODO
  }

  private static void checkPrincipal(Principal principal) {
    if (!"ADMIN".equals(principal.role())) {
      throw new ForbiddenException("Forbidden: wrong role=" + principal.role());
    }
  }
}
