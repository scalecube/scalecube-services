package io.scalecube.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("greeting")
public interface GreetingService {

  @ServiceMethod("one")
  Mono<String> hello(String name);


  @ServiceMethod("many")
  Flux<String> many(String name);

}
