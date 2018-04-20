package io.scalecube.services.transport;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import org.reactivestreams.Publisher;

@Service
public interface GreetingService {

  @ServiceMethod
  public Publisher<String> sayHello(String name);
  
  @ServiceMethod
  public Publisher<String> greetingChannel(Publisher<String> channel);
  
}
