package io.scalecube.services.a.b.testing;

import java.util.concurrent.CompletableFuture;


public final class GreetingServiceImplB implements CanaryService {

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture("B - hello to: " + name);
  }
}
