package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

@Service("io-sc-greeting")
public interface GreetingService {

  @ServiceMethod
  GreetingResponse greeting(GreetingRequest name);
 
  
  @ServiceMethod
  CompletableFuture<GreetingResponse> asyncGreeting(GreetingRequest name);
 
}
