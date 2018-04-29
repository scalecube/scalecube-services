package io.scalecube.services.a.b.testing;

import io.scalecube.services.GreetingRequest;
import io.scalecube.services.GreetingResponse;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Mono;


@Service
public interface CanaryService {

  @ServiceMethod
  Mono<GreetingResponse> greeting(GreetingRequest request);

}
