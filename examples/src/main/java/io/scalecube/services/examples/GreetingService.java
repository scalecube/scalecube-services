package io.scalecube.services.examples;

import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

@Service
public interface GreetingService {

  @ServiceMethod
  GreetingResponse greeting(GreetingRequest name);
 
  
  @ServiceMethod
  ListenableFuture<GreetingResponse> asyncGreeting(GreetingRequest name);
 
}
