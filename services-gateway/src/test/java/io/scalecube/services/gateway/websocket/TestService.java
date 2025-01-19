package io.scalecube.services.gateway.websocket;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface TestService {

  @ServiceMethod("manyNever")
  Flux<Long> manyNever();

  @ServiceMethod
  Mono<String> one(String one);

  @ServiceMethod
  Mono<String> oneErr(String one);
}
