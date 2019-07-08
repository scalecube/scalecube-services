package io.scalecube.services.sut.security;

import io.scalecube.services.exceptions.UnauthorizedException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  @Override
  public Mono<String> helloWithRequest(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> helloWithPrincipal(UserProfile user) {
    authorize(user);
    return Mono.just("Hello, " + user.name());
  }

  @Override
  public Mono<String> helloWithRequestAndPrincipal(String name, UserProfile user) {
    authorize(user);
    return Mono.just("Hello, " + name + " and " + user.name());
  }

  private void authorize(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new UnauthorizedException("Unathorized");
    }
  }
}
