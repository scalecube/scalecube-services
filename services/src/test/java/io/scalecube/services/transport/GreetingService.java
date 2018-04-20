package io.scalecube.services.transport;

import io.scalecube.services.annotations.Service;

import org.reactivestreams.Publisher;

@Service
public interface GreetingService {

  public Publisher<String> sayHello(String name);
  
  public Publisher<String> greetingChannel(Publisher<String> channel);
  
}
