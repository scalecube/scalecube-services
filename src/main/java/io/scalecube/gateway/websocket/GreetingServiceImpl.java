package io.scalecube.gateway.websocket;

import reactor.core.publisher.Mono;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public Mono<String> hello(String name) {
    return Mono.just("Echo:" + name);
  }
}
