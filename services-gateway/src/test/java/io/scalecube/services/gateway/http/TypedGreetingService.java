package io.scalecube.services.gateway.http;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.gateway.sut.typed.Shape;
import reactor.core.publisher.Mono;

@Service(TypedGreetingService.SERVICE_NAME)
public interface TypedGreetingService {

  String SERVICE_NAME = "v1/typed-greetings";

  @ServiceMethod
  Mono<Shape> helloPolymorph();

  @ServiceMethod
  Mono<Object> helloMultitype();

  @ServiceMethod
  Mono<?> helloWildcardMultitype();
}
