package io.scalecube.services.sut.typed;

import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service(TypedGreetingService.SERVICE_NAME)
public interface TypedGreetingService {

  String SERVICE_NAME = "v1/typed-greetings";

  @ServiceMethod
  Mono<Shape> helloPolymorph();

  @ServiceMethod
  Mono<Object> helloMultitype();
}
