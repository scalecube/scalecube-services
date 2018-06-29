package io.scalecube.services.methods;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface StubService {

  @ServiceMethod
  Mono<String> returnNull();

  @ServiceMethod
  Flux<String> returnNull2();

  @ServiceMethod
  Flux<String> returnNull3(Flux<String> request);

  @ServiceMethod
  Mono<String> throwException();

  @ServiceMethod
  Flux<String> throwException2();

  @ServiceMethod
  Flux<String> throwException3(Flux<String> request);

}
