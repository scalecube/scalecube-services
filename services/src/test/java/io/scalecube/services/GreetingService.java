package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

@Service
interface GreetingService {

  @ServiceMethod
  CompletableFuture<String> asyncGreeting(String string);

  @ServiceMethod
  CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request);

  @ServiceMethod
  CompletableFuture<GreetingResponse> asyncGreetingRequest(GreetingRequest string);

  @ServiceMethod
  CompletableFuture<Message> asyncGreetingMessage(Message request);

  @ServiceMethod
  void greetingVoid(GreetingRequest request);

}
