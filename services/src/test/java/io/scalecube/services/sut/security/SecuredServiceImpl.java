package io.scalecube.services.sut.security;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return Authenticator.deferSecured(CallerProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + user.name());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return Authenticator.deferSecured(CallerProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + name + " and " + user.name());
            });
  }

  @Override
  public Mono<String> helloWithRoles() {
    return null;
  }

  @Override
  public Mono<String> helloWithPermissions() {
    return null;
  }

  private void checkPrincipal(CallerProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
