package io.scalecube.services.sut.security;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class AnotherSecuredServiceImpl implements AnotherSecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return Mono.deferWithContext(context -> Mono.just(context.get(Authenticator.AUTH_CONTEXT_KEY)))
        .cast(UserProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + user.name());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return Mono.deferWithContext(context -> Mono.just(context.get(Authenticator.AUTH_CONTEXT_KEY)))
        .cast(UserProfile.class)
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
