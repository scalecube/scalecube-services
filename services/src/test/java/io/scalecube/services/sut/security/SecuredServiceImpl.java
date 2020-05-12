package io.scalecube.services.sut.security;

import io.scalecube.services.auth.PrincipalContext;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal() {
    return Mono.deferWithContext(this::userProfile)
        .flatMap(
            (UserProfile user) -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + user.name());
            });
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name) {
    return Mono.deferWithContext(this::userProfile)
        .flatMap(
            (UserProfile user) -> {
              checkPrincipal(user);
              return Mono.just("Hello, " + name + " and " + user.name());
            });
  }

  private void checkPrincipal(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }

  private Mono<UserProfile> userProfile(Context context) {
    return context.get(PrincipalContext.class).get();
  }
}
