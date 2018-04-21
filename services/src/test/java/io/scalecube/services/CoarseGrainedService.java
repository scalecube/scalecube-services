package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import org.reactivestreams.Publisher;

@Service
public interface CoarseGrainedService {

  @ServiceMethod
  public Publisher<String> callGreeting(String name);

  @ServiceMethod
  public Publisher<String> callGreetingTimeout(String request);

  @ServiceMethod
  public Publisher<String> callGreetingWithDispatcher(String request);

}
