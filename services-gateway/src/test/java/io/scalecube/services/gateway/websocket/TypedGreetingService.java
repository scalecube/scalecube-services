package io.scalecube.services.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.gateway.sut.typed.Shape;
import reactor.core.publisher.Flux;

@Service(TypedGreetingService.SERVICE_NAME)
public interface TypedGreetingService {

  String SERVICE_NAME = "v1/typed-greetings";

  @ServiceMethod
  Flux<Shape> helloPolymorph();

  @ServiceMethod
  Flux<Object> helloMultitype();

  @ServiceMethod
  Flux<?> helloWildcardMultitype();
}
