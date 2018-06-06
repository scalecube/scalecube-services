package io.scalecube.gateway.websocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public Mono<String> one(String name) {
    return Mono.just("Echo:" + name);
  }

  @Override
  public Mono<String> failingOne(String name) {
    return Mono.error(new RuntimeException(name));
  }

  @Override
  public Flux<String> many(String name) {
    return Flux.interval(Duration.ofMillis(100))
        .map(i -> "Greeting (" + i + ") to: " + name);
  }

  @Override
  public Flux<String> failingMany(String name) {
    return Flux.push(sink -> {
      sink.next("Echo:" + name);
      sink.next("Echo:" + name);
      sink.error(new RuntimeException("Echo:" + name));
    });
  }

  @Override
  public Mono<GreetingResponse> dtoOne(GreetingRequest request) {
    return one(request.getText()).map(GreetingResponse::new);
  }

  @Override
  public Flux<GreetingResponse> dtoMany(GreetingRequest request) {
    return many(request.getText()).map(GreetingResponse::new);
  }

}
