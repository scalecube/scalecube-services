package io.scalecube.services.sut.typed;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(TypedGreetingService.SERVICE_NAME)
public interface TypedGreetingService {

  String SERVICE_NAME = "v1/typed-greetings";

  @ServiceMethod
  Flux<Shape> helloPolymorph();

  @ServiceMethod
  Mono<List<Shape>> helloListPolymorph();

  @ServiceMethod
  Flux<Object> helloMultitype();

  @ServiceMethod
  Flux<?> helloWildcardMultitype();
}
