package io.scalecube.services.examples;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import java.util.List;
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

  @ServiceMethod("pojo/list")
  Mono<List<GreetingResponse>> pojoList(GreetingRequest request);

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

  @ServiceMethod("empty/pojo")
  Mono<EmptyGreetingResponse> emptyGreeting(EmptyGreetingRequest request);

  @RestMethod(qaulifier = "hello/:someVar", method = "POST")
  @ServiceMethod("empty/wrappedPojo")
  @RequestType(EmptyGreetingRequest.class)
  @ResponseType(EmptyGreetingResponse.class)
  Mono<ServiceMessage> emptyGreetingMessage(ServiceMessage request);

  @RestMethod(method = "POST")
  @ServiceMethod("hello/:someVar/:foo")
  Mono<String> helloDynamicQualifier(Long value);
}
