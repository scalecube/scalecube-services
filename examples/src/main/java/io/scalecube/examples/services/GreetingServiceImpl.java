package io.scalecube.examples.services;

import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

public class GreetingServiceImpl implements GreetingService {
  
  @PostConstruct
  void init(){
    System.out.println("invoke post constact");
  }
  
  @Override
  public CompletableFuture<String> greeting(String name) {
    System.out.println("Provider: 'greeting' -> " + name);
    return CompletableFuture.completedFuture("Hello " + name);
  }

  @Override
  public CompletableFuture<String> greetingException(String name) {
    System.out.println("Provider: 'greetingException' -> " + name);
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("greetingException"));
    return future;
  }

}
