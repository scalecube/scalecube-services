package io.scalecube.examples.services;

import java.util.concurrent.CompletableFuture;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public String syncGreeting(String name) {
    System.out.println("Provider: 'syncGreeting' -> " + name);
    return "Hello " + name;
  }

  @Override
  public CompletableFuture<String> asyncGreeting(String name) {
    System.out.println("Provider: 'asyncGreeting' -> " + name);
    return CompletableFuture.completedFuture("Hello " + name);
  }

  @Override
  public String syncGreetingException(String name) throws  UnsupportedOperationException {
    System.out.println("Provider: 'syncGreetingException' -> " + name);
    throw new UnsupportedOperationException("syncGreetingException");
  }

  @Override
  public CompletableFuture<String> asyncGreetingException(String name) {
    System.out.println("Provider: 'asyncGreetingException' -> " + name);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("asyncGreetingException"));
    return future;
  }

}
