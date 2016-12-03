package io.scalecube.services.a.b.testing;

import java.util.concurrent.CompletableFuture;


public final class GreetingServiceImplA implements CanaryService {

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture("A - hello to: " + name);
  }
}
