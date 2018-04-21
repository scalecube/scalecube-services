package io.scalecube.services.a.b.testing;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;


public final class GreetingServiceImplA implements CanaryService {

  @Override
  public Publisher<String> greeting(String name) {
    return Mono.just("A - hello to: " + name);
  }
}
