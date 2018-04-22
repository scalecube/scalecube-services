package io.scalecube.services;

import org.reactivestreams.Publisher;

import java.time.Duration;

import reactor.core.publisher.Mono;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  public static final String SERVICE_NAME = "io.scalecube.services.GreetingService";

  private GreetingService greetingServiceTimeout;

  private GreetingService greetingService;

  private Microservices ms;

  @Override
  public Publisher<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }

  @Override
  public Publisher<String> callGreetingTimeout(String request) {
    return Mono.from(this.greetingServiceTimeout.greetingRequestTimeout(new GreetingRequest(request, Duration.ofSeconds(2))))
        .map(response->response.getResult());
  }

  @Override
  public Publisher<String> callGreetingWithDispatcher(String request) {
    return Mono.from(ms.call().requestOne(Messages.builder().request(SERVICE_NAME, "greeting")
        .data("joe").build()))
        .map(response->response.data());
  }
}
