package io.scalecube.services;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import java.time.Duration;

import reactor.core.publisher.Mono;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  public static final String SERVICE_NAME = "io.scalecube.services.GreetingService";

  @Inject
  private GreetingService greetingServiceTimeout;

  @Inject
  private GreetingService greetingService;

  @Inject
  private Microservices microservices;

  @Override
  public Publisher<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }

  @Override
  public Publisher<String> callGreetingTimeout(String request) {
    return Mono
        .from(this.greetingServiceTimeout.greetingRequestTimeout(new GreetingRequest(request, Duration.ofSeconds(2))))
        .map(GreetingResponse::getResult);
  }

  @Override
  public Publisher<String> callGreetingWithDispatcher(String request) {
    return Mono.from(microservices.call().requestOne(Messages.builder().request(SERVICE_NAME, "greeting")
        .data("joe").build()))
        .map(ServiceMessage::data);
  }
}
