package io.scalecube.services.auth;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface PrincipalContext {

  <T> Mono<T> get();
}
