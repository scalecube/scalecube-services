package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;

@Service
interface GreetingService {

  @ServiceMethod
  CompletableFuture<String> greetingNoParams();

  @ServiceMethod
  CompletableFuture<String> greeting(String string);

  @ServiceMethod
  CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request);

  @ServiceMethod
  CompletableFuture<GreetingResponse> greetingRequest(GreetingRequest string);

  // @ServiceMethod
  // CompletableFuture<StreamMessage> greetingMessage(StreamMessage request);

  @ServiceMethod
  void greetingVoid(GreetingRequest request);

}
