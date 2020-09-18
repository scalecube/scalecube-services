package io.scalecube.services.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service
public interface Service2 {

  @ServiceMethod
  Mono<String> oneDelay(long interval);
}
