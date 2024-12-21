package io.scalecube.services.sut;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(GreetingService.SERVICE_NAME)
public interface GreetingService {

  String SERVICE_NAME = "v1/greetings";

  @ServiceMethod
  void notifyGreeting();

  @ServiceMethod
  Mono<String> greetingNoParams();

  @ServiceMethod
  Mono<String> greeting(String string);

  @ServiceMethod
  Mono<GreetingResponse> greetingPojo(GreetingRequest name);

  @ServiceMethod
  Mono<GreetingResponse> greetingNotAuthorized(GreetingRequest name);

  @ServiceMethod
  Mono<GreetingResponse> greetingRequestTimeout(GreetingRequest request);

  @ServiceMethod
  Mono<GreetingResponse> greetingRequest(GreetingRequest string);

  @ServiceMethod
  @RequestType(GreetingRequest.class)
  @ResponseType(GreetingResponse.class)
  Mono<ServiceMessage> greetingMessage(ServiceMessage request);

  @ServiceMethod
  @RequestType(GreetingRequest.class)
  Mono<GreetingResponse> greetingMessage2(ServiceMessage request);

  @ServiceMethod
  Mono<Void> greetingVoid(GreetingRequest request);

  @ServiceMethod
  Mono<Void> failingVoid(GreetingRequest request);

  @ServiceMethod
  Mono<Void> throwingVoid(GreetingRequest request);

  @ServiceMethod
  Mono<GreetingResponse> failingRequest(GreetingRequest request);

  @ServiceMethod
  Mono<GreetingResponse> exceptionRequest(GreetingRequest request);

  @ServiceMethod
  Mono<EmptyGreetingResponse> emptyGreeting(EmptyGreetingRequest request);

  @ServiceMethod
  @RequestType(EmptyGreetingRequest.class)
  @ResponseType(EmptyGreetingResponse.class)
  Mono<ServiceMessage> emptyGreetingMessage(ServiceMessage request);

  @ServiceMethod
  Flux<GreetingResponse> bidiGreeting(Publisher<GreetingRequest> request);

  @ServiceMethod
  @RequestType(GreetingRequest.class)
  @ResponseType(GreetingResponse.class)
  Flux<ServiceMessage> bidiGreetingMessage(Publisher<ServiceMessage> requests);

  @ServiceMethod
  Flux<GreetingResponse> bidiGreetingNotAuthorized(Flux<GreetingRequest> request);

  @ServiceMethod
  @RequestType(GreetingRequest.class)
  @ResponseType(GreetingResponse.class)
  Flux<ServiceMessage> bidiGreetingNotAuthorizedMessage(Publisher<ServiceMessage> requests);

  @ServiceMethod
  Flux<GreetingResponse> bidiGreetingIllegalArgumentException(Publisher<GreetingRequest> request);

  @ServiceMethod
  @RequestType(GreetingRequest.class)
  @ResponseType(GreetingResponse.class)
  Flux<ServiceMessage> bidiGreetingIllegalArgumentExceptionMessage(
      Publisher<ServiceMessage> requests);

  @ServiceMethod
  Mono<GreetingResponse> greetingMonoEmpty(GreetingRequest request);

  @ServiceMethod
  Flux<GreetingResponse> greetingFluxEmpty(GreetingRequest request);

  @ServiceMethod
  Flux<Long> manyStream(Long cnt);

  @ServiceMethod("hello/:someVar")
  Mono<String> helloDynamicQualifier(String value);
}
