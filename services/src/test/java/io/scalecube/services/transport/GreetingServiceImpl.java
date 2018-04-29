package io.scalecube.services.transport;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

public class GreetingServiceImpl implements GreetingService  {
  
  public Publisher<String> sayHello(String name){
    return Mono.just( "Hello to: " + name);
  }
  
  public Publisher<String> greetingChannel(Publisher<String> channel){
    return channel;
  }
}
