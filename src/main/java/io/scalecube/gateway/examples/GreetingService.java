package io.scalecube.gateway.examples;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("greeting")
public interface GreetingService {

  @ServiceMethod("one")
  Mono<String> one(String name);

  @ServiceMethod("many")
  Flux<String> many(String name);

  @ServiceMethod("manyStream")
  Flux<Integer> manyStream(Integer cnt);

  @ServiceMethod("failing/one")
  Mono<String> failingOne(String name);

  @ServiceMethod("failing/many")
  Flux<String> failingMany(String name);

  @ServiceMethod("pojo/one")
  Mono<GreetingResponse> pojoOne(GreetingRequest request);

  @ServiceMethod("pojo/many")
  Flux<GreetingResponse> pojoMany(GreetingRequest request);
}
