package io.scalecube.gateway.examples;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(GreetingService.NAMESPACE)
public interface GreetingService {

  String NAMESPACE = "greeting";

  @ServiceMethod("one")
  Mono<String> one(String name);

  @ServiceMethod("many")
  Flux<String> many(String name);

  @ServiceMethod("manyStream")
  Flux<Long> manyStream(Long cnt);

  @ServiceMethod("failing/one")
  Mono<String> failingOne(String name);

  @ServiceMethod("failing/many")
  Flux<String> failingMany(String name);

  @ServiceMethod("pojo/one")
  Mono<GreetingResponse> pojoOne(GreetingRequest request);

  @ServiceMethod("pojo/many")
  Flux<GreetingResponse> pojoMany(GreetingRequest request);

  @ServiceMethod("empty/one")
  Mono<String> emptyOne(String name);

  @ServiceMethod("empty/many")
  Flux<String> emptyMany(String name);

  @ServiceMethod("never/one")
  Mono<String> neverOne(String name);

  @ServiceMethod("delay/one")
  Mono<String> delayOne(String name);

  @ServiceMethod("delay/many")
  Flux<String> delayMany(String name);
}
