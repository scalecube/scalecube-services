package io.scalecube.services.transport;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface GreetingService {

  @ServiceMethod
  Mono<String> sayHello(String name);
  
  @ServiceMethod
  Flux<String> greetingChannel(Flux<String> channel);
  
}
