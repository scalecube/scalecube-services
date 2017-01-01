package io.scalecube.services;

import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class GreetingServiceImpl implements GreetingService {

  @Override
  public String toString() {
    return "GreetingServiceImpl []";
  }

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture(" hello to: " + name);
  }

  @Override
  public CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    CompletableFuture<GreetingResponse> response = new CompletableFuture<GreetingResponse>();

    Executors.newScheduledThreadPool(1).schedule(() -> {
      try {
        response.complete(new GreetingResponse(" hello to: " + request.getName()));
      } catch (Exception ex) {
      }
    }, request.getDuration().toMillis(), TimeUnit.MILLISECONDS);

    return response;
  }

  @Override
  public CompletableFuture<String> greetingNoParams() {
    return CompletableFuture.completedFuture("hello unknown");
  }

  @Override
  public CompletableFuture<GreetingResponse> greetingRequest(GreetingRequest request) {
    return CompletableFuture.completedFuture(new GreetingResponse(" hello to: " + request.getName()));
  }

  @Override
  public CompletableFuture<Message> greetingMessage(Message request) {
    return CompletableFuture.completedFuture(Message.fromData(" hello to: " + request.data()));
  }

  @Override
  public void greetingVoid(GreetingRequest request) {
    System.out.println(" hello to: " + request.getName());
  }
}
