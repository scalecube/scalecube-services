package io.scalecube.services.gateway;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class EchoServiceImpl implements EchoService {

  @Override
  public Mono<String> echoOne(String request) {
    return Mono.just(request);
  }

  @Override
  public Mono<String> failOne(String request) {
    return Mono.error(new SomeException());
  }

  @Override
  public Flux<String> echo(String request) {
    return Flux.just(request);
  }

  @Override
  public Flux<String> fail(String request) {
    return Flux.error(new SomeException());
  }
}
