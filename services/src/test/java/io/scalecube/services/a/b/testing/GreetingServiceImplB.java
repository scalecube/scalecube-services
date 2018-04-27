package io.scalecube.services.a.b.testing;

import io.scalecube.services.GreetingRequest;
import io.scalecube.services.GreetingResponse;

import reactor.core.publisher.Mono;


public final class GreetingServiceImplB implements CanaryService {

  @Override
  public Mono<GreetingResponse> greeting(GreetingRequest name) {
    return Mono.just(new GreetingResponse("SERVICE_B_TALKING - hello to: " + name));
  }
}
