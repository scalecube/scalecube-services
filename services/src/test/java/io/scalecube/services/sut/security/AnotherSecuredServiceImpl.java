package io.scalecube.services.sut.security;

import io.scalecube.services.auth.MonoAuthUtil;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class AnotherSecuredServiceImpl implements AnotherSecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return MonoAuthUtil.deferWithPrincipal(UserProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + user.name());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return MonoAuthUtil.deferWithPrincipal(UserProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + name + " and " + user.name());
            });
  }

  private void checkPrincipal(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
