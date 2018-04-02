package io.scalecube.examples.services;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.Inject;

import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

public class GreetingServiceImpl implements GreetingService {

  @Inject
  Microservices ms;

  @PostConstruct
  private void construct() {
    System.out.println("Construct service: GreetingService@" + ms.cluster().member().id());
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
