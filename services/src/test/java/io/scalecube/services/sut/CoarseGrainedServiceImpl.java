package io.scalecube.services.sut;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;
import reactor.core.publisher.Mono;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  @Inject public GreetingService greetingServiceTimeout;

  @Inject private GreetingService greetingService;

  @Inject private Microservices microservices;

  @Override
  public Mono<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }

  @Override
  public Mono<String> callGreetingTimeout(String request) {
    System.out.println("callGreetingTimeout: " + request);
    return Mono.from(
            this.greetingServiceTimeout.greetingRequestTimeout(
                new GreetingRequest(request, Duration.ofSeconds(3))))
        .timeout(Duration.ofSeconds(1))
        .map(GreetingResponse::getResult);
  }

  @Override
  public Mono<String> callGreetingWithDispatcher(String request) {
    return microservices
        .call()
        .create()
        .requestOne(
            ServiceMessage.builder()
                .qualifier(GreetingService.SERVICE_NAME, "greeting")
                .data("joe")
                .build())
        .map(ServiceMessage::data);
  }
}
