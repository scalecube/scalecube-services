package io.scalecube.services.sut.security;

import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal(UserProfile user) {
    checkPrincipal(user);
    return Mono.just("Hello, " + user.name());
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name, UserProfile user) {
    checkPrincipal(user);
    return Mono.just("Hello, " + name + " and " + user.name());
  }

  private void checkPrincipal(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
