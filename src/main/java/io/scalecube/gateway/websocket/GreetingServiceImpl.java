package io.scalecube.gateway.websocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public Mono<String> hello(String name) {
    return Mono.just("Echo:" + name);
  }

  @Override
  public Flux<String> many(String name) {
    return Flux.interval(Duration.ofSeconds(1))
        .map(i -> "Greeting (" + i + ") to: " + name);
  }

  public Flux<String> helloStream(Flux<String> name) {
    return name.map(s -> "ECHO:" + s);
  }
}
