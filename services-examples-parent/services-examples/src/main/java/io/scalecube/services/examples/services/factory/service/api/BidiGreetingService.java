package io.scalecube.services.examples.services.factory.service.api;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("BidiGreeting")
public interface BidiGreetingService {

  @ServiceMethod()
  Mono<String> greeting();
}
