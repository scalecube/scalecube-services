package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

public class HelloWorldComponent implements GreetingService{

  @Override
  public GreetingResponse greeting(GreetingRequest req) {
    System.out.println("greeting call accepted");
    return new GreetingResponse(" hello to: " + req.name());
  }

  @Override
  public CompletableFuture<GreetingResponse> asyncGreeting(GreetingRequest req) {
    System.out.println("asyncGreeting call accepted");
    return CompletableFuture.completedFuture(new GreetingResponse(" hello to: " + req.name()));
  }
  
}
