package io.scalecube.services.gateway.websocket;

import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestServiceImpl implements TestService {

  private final Runnable onClose;

  public TestServiceImpl(Runnable onClose) {
    this.onClose = onClose;
  }

  @Override
  public Flux<Long> manyNever() {
    return Flux.<Long>never().log(">>>").doOnCancel(onClose);
  }

  @Override
  public Mono<String> one(String one) {
    return Mono.just(one);
  }

  @Override
  public Mono<String> oneErr(String one) {
    throw new ForbiddenException("forbidden");
  }
}
