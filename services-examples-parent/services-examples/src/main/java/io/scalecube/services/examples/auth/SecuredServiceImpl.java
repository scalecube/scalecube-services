package io.scalecube.services.examples.auth;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;

import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> securedHello(String name) {
    return deferWithContext()
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, name=" + name + " and user.name=" + user.name());
            });
  }

  private Mono<UserProfile> deferWithContext() {
    return Mono.deferWithContext(context -> Mono.just(context.get(AUTH_CONTEXT_KEY)))
        .cast(UserProfile.class);
  }

  private void checkPrincipal(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
