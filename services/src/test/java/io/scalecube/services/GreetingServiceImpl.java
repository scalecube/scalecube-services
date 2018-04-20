package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Mono;

final class GreetingServiceImpl implements GreetingService {

  Microservices ms;

  @Override
  public String toString() {
    return "GreetingServiceImpl []";
  }

  @Override
  public Publisher<String> greeting(String name) {
    return Mono.just(" hello to: " + name);
  }


  @Override
  public Publisher<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    return Mono.just(new GreetingResponse(" hello to: " + request.getName(), String.valueOf(this.hashCode())));
  }

  @Override
  public org.reactivestreams.Publisher<String> greetingNoParams() {
    return Mono.just("hello unknown");
  }

  @Override
  public Publisher<GreetingResponse> greetingRequest(GreetingRequest request) {
    return Mono.just(new GreetingResponse(" hello to: " + request.getName(), "1"));
  }

  @Override
  public Publisher<ServiceMessage> greetingMessage(ServiceMessage request) {
    GreetingResponse resp = new GreetingResponse(" hello to: " + request.data(), "1");
    return Mono.just(ServiceMessage.builder().data(resp).build());
  }

  @Override
  public void greetingVoid(GreetingRequest request) {
    System.out.println(" hello to: " + request.getName());
  }
}
