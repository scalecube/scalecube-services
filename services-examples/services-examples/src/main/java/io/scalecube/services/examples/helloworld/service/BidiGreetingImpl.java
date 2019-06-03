package io.scalecube.services.examples.helloworld.service;

import io.scalecube.services.examples.helloworld.service.api.BidiGreetingService;
import reactor.core.publisher.Flux;

/**
 * Greeting is an act of communication in which human beings intentionally make their presence known
 * to each other, to show attention to, and to suggest a type of relationship (usually cordial) or
 * social status (formal or informal) between individuals or groups of people coming in contact with
 * each other.
 */
public class BidiGreetingImpl implements BidiGreetingService {

  /**
   * Call this method to be greeted by the this ScaleCube service.
   *
   * @param requestStream incoming stream of names to greet.
   * @return service greeting
   */
  @Override
  public Flux<String> greeting(Flux<String> requestStream) {
    return requestStream.map(next -> "greeting: " + next);
  }
}
