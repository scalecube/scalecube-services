package io.scalecube.services.sut.security;

import reactor.core.publisher.Mono;

public class PartiallySecuredServiceImpl implements PartiallySecuredService {

  @Override
  public Mono<String> publicMethod(String name) {
    return Mono.just("Hello, " + name);
  }

  @Override
  public Mono<String> securedMethod(String name) {
    return Mono.just("Hello, " + name);
  }
}
