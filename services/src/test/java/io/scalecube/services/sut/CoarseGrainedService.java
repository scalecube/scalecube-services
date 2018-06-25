package io.scalecube.services.sut;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Mono;

@Service
public interface CoarseGrainedService {

  @ServiceMethod
  Mono<String> callGreeting(String name);

  @ServiceMethod
  Mono<String> callGreetingTimeout(String request);

  @ServiceMethod
  Mono<String> callGreetingWithDispatcher(String request);

}
