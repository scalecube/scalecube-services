package io.scalecube.services.stress;

import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture("hello to: " + name);
  }
  
  @Override
  public CompletableFuture<Message> greetingMessage(Message request) {
    return CompletableFuture.completedFuture(Message.fromData("hello to: " + request.data()));
  }

}
