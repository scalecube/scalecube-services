package io.scalecube.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.streams.StreamMessage;

import java.util.concurrent.CompletableFuture;

@Service("scalecube-greeting-service")
public interface GreetingService {

  @ServiceMethod
  CompletableFuture<String> greeting(String string);

  @ServiceMethod
  <REQ, RESP> CompletableFuture<StreamMessage<Integer>> messagesGreeting(StreamMessage<String> req);

  @ServiceMethod
  CompletableFuture<String> greetingException(String name);

}
