package io.scalecube.services;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.RoundRobinServiceRouter;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  @ServiceProxy(router = RoundRobinServiceRouter.class, timeout = 3, timeUnit = TimeUnit.SECONDS)
  private GreetingService greetingService;

  @Inject
  private Microservices ms;

  @Override
  public CompletableFuture<String> callGreeting(String name) {
    if (ms != null) {
      return this.greetingService.greeting(name);
    } else {
      return CompletableFuture.completedFuture("microservices is not set");
    }
  }

}
