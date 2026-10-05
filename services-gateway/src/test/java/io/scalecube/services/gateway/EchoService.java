package io.scalecube.services.gateway;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface EchoService {

  @ServiceMethod
  Mono<String> echoOne(String request);

  @ServiceMethod
  Mono<String> failOne(String request);

  @ServiceMethod
  Flux<String> echo(String request);

  @ServiceMethod
  Flux<String> fail(String request);
}
