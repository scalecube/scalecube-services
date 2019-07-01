package io.scalecube.services.sut;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.UnauthorizedException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class GreetingServiceImpl implements GreetingService {

  @Inject Microservices ms;

  private int instanceId;

  private boolean ci = System.getenv("TRAVIS") != null;

  public GreetingServiceImpl() {}

  public GreetingServiceImpl(int id) {
    this.instanceId = id;
  }

  @Override
  public String toString() {
    return "GreetingServiceImpl []";
  }

  @Override
  public Mono<String> greeting(String name) {
    return Mono.just(" hello to: " + name);
  }

  @Override
  public Mono<GreetingResponse> greetingPojo(GreetingRequest name) {
    return Mono.just(new GreetingResponse(" hello to: " + name));
  }

  @Override
  public Mono<GreetingResponse> greetingNotAuthorized(GreetingRequest name) {
    return Mono.error(new UnauthorizedException(500, "Not authorized"));
  }

  @Override
  public Mono<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    print("[greetingRequestTimeout] Hello... i am a service an just recived a message:" + request);
    return Mono.delay(request.getDuration())
        .flatMap(
            i ->
                Mono.just(
                    new GreetingResponse(
                        " hello to: " + request.getName(), String.valueOf(this.hashCode()))));
  }

  @Override
  public Mono<String> greetingNoParams() {
    print(
        "[greetingNoParams] Hello... i am a service an just recived "
            + "a call bu i dont know from who.");
    return Mono.just("hello unknown");
  }

  @Override
  public Mono<GreetingResponse> greetingRequest(GreetingRequest request) {
    print(
        instanceId
            + ":[greetingRequest] Hello... i am a service an just recived a message:"
            + request);
    return Mono.just(new GreetingResponse(" hello to: " + request.getName(), "" + instanceId));
  }

  @Override
  public Flux<GreetingResponse> bidiGreeting(Publisher<GreetingRequest> request) {
    return Flux.from(request)
        .map(onNext -> new GreetingResponse(" hello to: " + onNext.getName(), "" + instanceId));
  }

  @Override
  public Flux<GreetingResponse> bidiGreetingNotAuthorized(Flux<GreetingRequest> request) {
    return Flux.error(new UnauthorizedException(500, "Not authorized"));
  }

  @Override
  public Flux<GreetingResponse> bidiGreetingIllegalArgumentException(
      Publisher<GreetingRequest> request) {
    throw new IllegalArgumentException("IllegalArgumentException");
  }

  @Override
  public Mono<GreetingResponse> greetingMonoEmpty(GreetingRequest request) {
    return Mono.empty();
  }

  @Override
  public Flux<GreetingResponse> greetingFluxEmpty(GreetingRequest request) {
    return Flux.empty();
  }

  @Override
  public Mono<ServiceMessage> greetingMessage(ServiceMessage request) {
    print("[greetingMessage] Hello... i am a service an just recived a message:" + request);
    GreetingResponse resp = new GreetingResponse(" hello to: " + request.data(), "1");
    return Mono.just(ServiceMessage.builder().data(resp).build());
  }

  @Override
  public Mono<Void> greetingVoid(GreetingRequest request) {
    print("[greetingVoid] Hello... i am a service an just recived a message:" + request);
    print(" hello to: " + request.getName());
    return Mono.empty();
  }

  @Override
  public Mono<Void> failingVoid(GreetingRequest request) {
    print("[failingVoid] Hello... i am a service an just recived a message:" + request);
    return Mono.error(new IllegalArgumentException(request.toString()));
  }

  @Override
  public Mono<Void> throwingVoid(GreetingRequest request) {
    print("[failingVoid] Hello... i am a service an just recived a message:" + request);
    throw new IllegalArgumentException(request.toString());
  }

  @Override
  public Mono<GreetingResponse> failingRequest(GreetingRequest request) {
    print("[failingRequest] Hello... i am a service an just recived a message:" + request);
    return Mono.error(new IllegalArgumentException(request.toString()));
  }

  @Override
  public Mono<GreetingResponse> exceptionRequest(GreetingRequest request) {
    print("[exceptionRequest] Hello... i am a service an just recived a message:" + request);
    throw new IllegalArgumentException(request.toString());
  }

  @Override
  public void notifyGreeting() {
    print("[notifyGreeting] Hello... i am a service and i just notefied");
  }

  private void print(String message) {
    if (!ci) {
      System.out.println(message);
    }
  }
}
