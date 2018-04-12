package io.scalecube.services;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.RoundRobinServiceRouter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;



public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  public static final String SERVICE_NAME = "io.scalecube.services.GreetingService";

  @ServiceProxy(router = RoundRobinServiceRouter.class, timeout = 1, timeUnit = TimeUnit.MILLISECONDS)
  private GreetingService greetingServiceTimeout;

  @Inject
  private GreetingService greetingService;

  @Inject
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
