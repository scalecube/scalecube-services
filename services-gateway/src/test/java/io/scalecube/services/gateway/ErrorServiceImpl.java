package io.scalecube.services.gateway;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ErrorServiceImpl implements ErrorService {

  @Override
  public Flux<Long> manyError() {
    return Flux.error(new SomeException());
  }

  @Override
  public Mono<String> oneError() {
    return Mono.error(new SomeException());
  }
}
