package io.scalecube.services.transport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GreetingServiceImpl implements GreetingService  {
  
  public Mono<String> sayHello(String name) {
    return Mono.just( "Hello to: " + name);
  }
  
  public Flux<String> greetingChannel(Flux<String> channel) {
    return channel;
  }
}
