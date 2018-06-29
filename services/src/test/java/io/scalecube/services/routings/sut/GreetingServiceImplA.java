package io.scalecube.services.routings.sut;

import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;

import reactor.core.publisher.Mono;


public final class GreetingServiceImplA implements CanaryService {

  @Override
  public Mono<GreetingResponse> greeting(GreetingRequest name) {
    return Mono.just(new GreetingResponse("SERVICE_A_TALKING - hello to: " + name));
  }
}
