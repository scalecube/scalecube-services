package io.scalecube.services.examples.helloworld.service;

import io.scalecube.services.examples.helloworld.service.api.Greeting;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import reactor.core.publisher.Mono;

public class GreetingServiceImpl implements GreetingsService {
  @Override
  public Mono<Greeting> sayHello(String name) {
    return Mono.just(new Greeting("Nice to meet you " + name + " and welcome to ScaleCube"));
  }
}
