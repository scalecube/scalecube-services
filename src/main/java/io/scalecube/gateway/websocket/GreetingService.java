package io.scalecube.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("greeting")
public interface GreetingService {

  ServiceMessage GREETING_ONE =
      ServiceMessage.builder().qualifier("/greeting/one").data("hello").build();
  ServiceMessage GREETING_FAILING_ONE =
      ServiceMessage.builder().qualifier("/greeting/failing/one").data("hello").build();
  ServiceMessage GREETING_MANY =
      ServiceMessage.builder().qualifier("/greeting/many").data("hello").build();
  ServiceMessage GREETING_FAILING_MANY =
      ServiceMessage.builder().qualifier("/greeting/failing/many").data("hello").build();

  @ServiceMethod("one")
  Mono<String> hello(String name);

  @ServiceMethod("failing/one")
  Mono<String> failingOne(String name);

  @ServiceMethod("many")
  Flux<String> many(String name);

  @ServiceMethod("failing/many")
  Flux<String> failingMany(String name);


}
