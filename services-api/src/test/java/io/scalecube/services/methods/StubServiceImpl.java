package io.scalecube.services.methods;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StubServiceImpl implements StubService {

  @Override
  public Mono<String> returnNull() {
    return null;
  }

  @Override
  public Flux<String> returnNull2() {
    return null;
  }

  @Override
  public Flux<String> returnNull3(Flux<String> request) {
    return null;
  }

  @Override
  public Mono<String> throwException() {
    throw new RuntimeException();
  }

  @Override
  public Flux<String> throwException2() {
    throw new RuntimeException();
  }

  @Override
  public Flux<String> throwException3(Flux<String> request) {
    throw new RuntimeException();
  }

}
