package io.scalecube.services.examples.services.factory.service;

import io.scalecube.services.examples.services.factory.service.api.BidiGreetingService;
import io.scalecube.services.examples.services.factory.service.api.Greeting;
import io.scalecube.services.examples.services.factory.service.api.GreetingsService;
import reactor.core.publisher.Mono;

/**
 * Greeting is an act of communication in which human beings intentionally make their presence known
 * to each other, to show attention to, and to suggest a type of relationship (usually cordial) or
 * social status (formal or informal) between individuals or groups of people coming in contact with
 * each other.
 */
public class BidiGreetingImpl implements BidiGreetingService {

  private final GreetingsService greetingsService;

  public BidiGreetingImpl(GreetingsService greetingsService) {
    this.greetingsService = greetingsService;
  }

  /**
   * Call this method to be greeted by the this ScaleCube service.
   *
   * @return service greeting
   */
  @Override
  public Mono<String> greeting() {
    return this.greetingsService.sayHello("Jack").map(Greeting::message);
  }
}
