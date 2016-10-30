package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

@Service
public interface GreetingService {

  @ServiceMethod
  String greeting(String name);

  @ServiceMethod
  CompletableFuture<String> asyncGreeting(String string);

  
  @ServiceMethod()
  GreetingResponse greetingRequest(GreetingRequest request);

  @ServiceMethod
  CompletableFuture<GreetingResponse> asyncGreetingRequest(GreetingRequest string);
 
  @ServiceMethod()
  Message greetingMessage(Message request);

  @ServiceMethod
  CompletableFuture<Message> asyncGreetingMessage(Message request);
  
}
