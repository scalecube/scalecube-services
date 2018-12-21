package io.scalecube.services.examples.exceptions;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("example.serviceB")
public interface ServiceB {

  @ServiceMethod
  Mono<Integer> doAnotherStuff(int input);
}
