package io.scalecube.services;

import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

final class GreetingServiceImpl implements GreetingService {

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
    return new GreetingResponse(" hello to: " + request.getName());
  }

  @Override
  public CompletableFuture<GreetingResponse> asyncGreetingRequest(GreetingRequest request) {
    return CompletableFuture.completedFuture(new GreetingResponse(" hello to: " + request.getName()));
  }

  @Override
  public Message greetingMessage(Message request) {
    return Message.fromData(" hello to: " + request.data());
  }

  @Override
  public CompletableFuture<Message> asyncGreetingMessage(Message request) {
    return CompletableFuture.completedFuture(Message.fromData(" hello to: " + request.data()));
  }

}
