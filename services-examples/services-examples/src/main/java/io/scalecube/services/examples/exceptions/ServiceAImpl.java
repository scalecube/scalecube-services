package io.scalecube.services.examples.exceptions;

import reactor.core.publisher.Mono;

public class ServiceAImpl implements ServiceA {

  @Override
  public Mono<Integer> doStuff(int input) {
    if (input == 0) {
      return Mono.error(new ServiceAException("Input is zero"));
    }

    return Mono.just(input);
  }
}
