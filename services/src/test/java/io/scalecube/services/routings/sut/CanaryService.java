package io.scalecube.services.routings.sut;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;

import reactor.core.publisher.Mono;


@Service
public interface CanaryService {

  @ServiceMethod
  Mono<GreetingResponse> greeting(GreetingRequest request);

}
