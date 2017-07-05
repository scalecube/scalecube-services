package io.scalecube.services;

import java.util.concurrent.CompletableFuture;

import io.scalecube.services.annotations.ServiceProxy;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  @ServiceProxy
  private GreetingService greetingService;

  @Override
  public CompletableFuture<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }
}
