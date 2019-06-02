package io.scalecube.services.examples.exceptions;

import reactor.core.publisher.Mono;

public class ServiceBImpl implements ServiceB {

  private ServiceA serviceA;

  ServiceBImpl(ServiceA serviceA) {
    this.serviceA = serviceA;
  }

  @Override
  public Mono<Integer> doAnotherStuff(int input) {
    return serviceA
        .doStuff(input)
        .doOnError(
            ServiceAException.class,
            th ->
                System.err.println(
                    "Service client mapper is defined for for ServiceA, "
                        + "so exact ServiceAException instance can be caught! -> "
                        + th));
  }
}
