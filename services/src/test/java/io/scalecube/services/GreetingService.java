package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

@Service
interface GreetingService {

  @ServiceMethod
  CompletableFuture<String> greeting(String string);

  @ServiceMethod
  CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request);

  @ServiceMethod
  CompletableFuture<GreetingResponse> greetingRequest(GreetingRequest string);

  @ServiceMethod
  CompletableFuture<Message> greetingMessage(Message request);

  @ServiceMethod
  void greetingVoid(GreetingRequest request);

}
