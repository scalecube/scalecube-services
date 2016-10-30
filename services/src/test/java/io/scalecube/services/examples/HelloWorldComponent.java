package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

import io.scalecube.transport.Message;

public class HelloWorldComponent implements GreetingService {
  @Override
  public String greeting(String name) {
    return " hello to: " + name;
  }

  @Override
  public CompletableFuture<String> asyncGreeting(String name) {
    return CompletableFuture.completedFuture(" hello to: " + name);
  }

  @Override
  public GreetingResponse greetingRequest(GreetingRequest request) {
    return new GreetingResponse(" hello to: " + request.name());
  }

  @Override
  public CompletableFuture<GreetingResponse> asyncGreetingRequest(GreetingRequest request) {
    return CompletableFuture.completedFuture(new GreetingResponse(" hello to: " + request.name()));
  }

  @Override
  public Message greetingMessage(Message request) {
    return Message.builder().data(
        " hello to: " + request.data() 
        ).build();
  }

  @Override
  public CompletableFuture<Message> asyncGreetingMessage(Message request) { 
    return CompletableFuture.completedFuture(
        Message.builder().data(" hello to: " + request.data()).build()
        );
  }

}
