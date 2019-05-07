package io.scalecube.services.examples.helloworld.service.api;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;

@Service("BidiGreeting")
public interface BidiGreetingService {

  @ServiceMethod()
  public Flux<String> greeting(Flux<String> request);
  
}
