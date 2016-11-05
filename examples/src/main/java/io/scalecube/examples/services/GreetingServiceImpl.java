package io.scalecube.examples.services;

import java.util.concurrent.CompletableFuture;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public String syncGreeting(String name) {
    System.out.println("Request to 'syncGreeting' -> " + name);
    return "Hello " + name;
  }

  @Override
  public CompletableFuture<String> asyncGreeting(String name) {
    System.out.println("Request to 'asyncGreeting' -> " + name);
    return CompletableFuture.completedFuture("Hello " + name);
  }

  @Override
  public String syncGreetingException(String name) {
    System.out.println("Request to 'syncGreetingException' -> " + name);
    throw new UnsupportedOperationException("syncGreetingException");
  }

  @Override
  public CompletableFuture<String> asyncGreetingException(String name) {
    System.out.println("Request to 'asyncGreetingException' -> " + name);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("asyncGreetingException"));
    return future;
  }

}
