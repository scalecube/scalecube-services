package io.scalecube.services.gateway;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface ErrorService {

  @ServiceMethod
  Flux<Long> manyError();

  @ServiceMethod
  Mono<String> oneError();
}
