package io.scalecube.services;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;


public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  public static final String SERVICE_NAME = "io.scalecube.services.GreetingService";

  private GreetingService greetingServiceTimeout;

  private GreetingService greetingService;

  private Microservices ms;

  @Override
  public CompletableFuture<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }

  @Override
  public CompletableFuture<String> callGreetingTimeout(String request) {
    CompletableFuture<String> future = new CompletableFuture<String>();
    this.greetingServiceTimeout.greetingRequestTimeout(new GreetingRequest(request, Duration.ofSeconds(2)))
        .whenComplete((success, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          } else {
            future.complete(success.getResult());
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<String> callGreetingWithDispatcher(String request) {
    final CompletableFuture<String> future = new CompletableFuture<String>();

    ms.call().invoke(Messages.builder().request(SERVICE_NAME, "greeting")
        .data("joe").build()).whenComplete((message, error) -> {
          future.complete(message.data().toString());
        });

    return future;
  }
}
