package io.scalecube.services.examples;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class HelloWorldComponent implements GreetingService{

  @Override
  public GreetingResponse greeting(GreetingRequest req) {
    return new GreetingResponse(" hello to: " + req.name());
  }

  @Override
  public ListenableFuture<GreetingResponse> asyncGreeting(GreetingRequest req) {
    return Futures.immediateFuture(new GreetingResponse(" hello to: " + req.name()));
  }
  
}
