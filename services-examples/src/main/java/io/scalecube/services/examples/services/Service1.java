package io.scalecube.services.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;

@Service
public interface Service1 {

  @ServiceMethod
  Flux<String> manyDelay(long interval);

  @ServiceMethod
  Flux<String> remoteCallThenManyDelay(long interval);
}
