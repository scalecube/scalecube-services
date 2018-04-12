package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;

@Service
public interface CoarseGrainedService {

  @ServiceMethod
  public CompletableFuture<String> callGreeting(String name);

  @ServiceMethod
  public CompletableFuture<String> callGreetingTimeout(String request);

  @ServiceMethod
  public CompletableFuture<String> callGreetingWithDispatcher(String request);

}
