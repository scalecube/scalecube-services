package io.scalecube.services;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

final class GreetingServiceImpl implements GreetingService {

  @Inject
  Microservices ms;

  private int instanceId;

  public GreetingServiceImpl() {
  }

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
  public Publisher<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    System.out.println("[greetingRequestTimeout] Hello... i am a service an just recived a message:" + request);
    return Mono.delay(request.getDuration()).flatMap(
        i -> Mono.just(new GreetingResponse(" hello to: " + request.getName(), String.valueOf(this.hashCode()))));
  }

  @Override
  public Publisher<String> greetingNoParams() {
    System.out.println("[greetingNoParams] Hello... i am a service an just recived a call bu i dont know from who.");
    return Mono.just("hello unknown");
  }

  @Override
  public Publisher<GreetingResponse> greetingRequest(GreetingRequest request) {
    System.out.println(instanceId + ":[greetingRequest] Hello... i am a service an just recived a message:" + request);
    return Mono.just(new GreetingResponse(" hello to: " + request.getName(), "" + instanceId));
  }

  @Override
  public Publisher<ServiceMessage> greetingMessage(ServiceMessage request) {
    System.out.println("[greetingMessage] Hello... i am a service an just recived a message:" + request);
    GreetingResponse resp = new GreetingResponse(" hello to: " + request.data(), "1");
    return Mono.just(ServiceMessage.builder().data(resp).build());
  }

  @Override
  public Mono<Void> greetingVoid(GreetingRequest request) {
    System.out.println("[greetingVoid] Hello... i am a service an just recived a message:" + request);
    System.out.println(" hello to: " + request.getName());
    return Mono.empty();
  }

  @Override
  public Mono<Void> failingVoid(GreetingRequest request) {
    System.out.println("[failingVoid] Hello... i am a service an just recived a message:" + request);
    return Mono.error(new IllegalArgumentException(request.toString()));
  }

  @Override
  public Mono<Void> exceptionVoid(GreetingRequest request) {
    System.out.println("[exceptionVoid] Hello... i am a service an just recived a message:" + request);
    throw new IllegalArgumentException(request.toString());
  }
}
