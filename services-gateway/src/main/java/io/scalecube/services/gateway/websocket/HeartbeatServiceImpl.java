package io.scalecube.services.gateway.websocket;

import reactor.core.publisher.Mono;

public class HeartbeatServiceImpl implements HeartbeatService {

  @Override
  public Mono<Long> ping(long value) {
    return Mono.just(value);
  }
}
