package io.scalecube.services.sut.typed;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(TypedGreetingService.SERVICE_NAME)
public interface TypedGreetingService {

  String SERVICE_NAME = "v1/typed-greetings";

  @ServiceMethod
  Flux<Shape> helloPolymorph();

  @ServiceMethod
  Flux<Object> helloMultitype(String t);

  @ServiceMethod
  Flux<?> helloWildcardMultitype(String t);

  @ServiceMethod
  Mono<Object[]> helloMultitypeArray(String t);
}
