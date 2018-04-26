package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@Service
interface GreetingService {

  @ServiceMethod
  Publisher<String> greetingNoParams();

  @ServiceMethod
  Publisher<String> greeting(String string);

  @ServiceMethod
  Publisher<GreetingResponse> greetingRequestTimeout(GreetingRequest request);

  @ServiceMethod
  Publisher<GreetingResponse> greetingRequest(GreetingRequest string);

  @ServiceMethod
  Publisher<ServiceMessage> greetingMessage(ServiceMessage request);

  @ServiceMethod
  Mono<Void> greetingVoid(GreetingRequest request);

  @ServiceMethod
  Mono<Void> failingVoid(GreetingRequest request);

  @ServiceMethod
  Mono<Void> exceptionVoid(GreetingRequest request);
}
