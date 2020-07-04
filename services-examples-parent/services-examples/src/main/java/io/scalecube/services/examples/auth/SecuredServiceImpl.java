package io.scalecube.services.examples.auth;

import io.scalecube.services.auth.MonoAuthUtil;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> securedHello(String name) {
    return MonoAuthUtil.deferWithPrincipal(UserProfile.class)
        .flatMap(
            user -> {
              checkPrincipal(user);
              return Mono.just("Hello, name=" + name + " and user.name=" + user.name());
            });
  }

  private void checkPrincipal(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
