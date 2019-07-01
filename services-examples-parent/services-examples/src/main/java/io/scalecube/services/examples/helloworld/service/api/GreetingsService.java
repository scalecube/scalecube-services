package io.scalecube.services.examples.helloworld.service.api;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

/**
 * Greeting is an act of communication in which human beings intentionally make their presence known
 * to each other, to show attention to, and to suggest a type of relationship (usually cordial) or
 * social status (formal or informal) between individuals or groups of people coming in contact with
 * each other.
 */
@Service("io.scalecube.Greetings")
public interface GreetingsService {
  /**
   * Call this method to be greeted by the this ScaleCube service.
   *
   * @param name name of the caller
   * @return service greeting
   */
  @ServiceMethod("sayHello")
  Mono<Greeting> sayHello(String name);
}
