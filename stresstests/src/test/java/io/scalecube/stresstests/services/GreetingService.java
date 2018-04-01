package io.scalecube.stresstests.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.streams.StreamMessage;

import java.util.concurrent.CompletableFuture;

@Service
public interface GreetingService {

  @ServiceMethod
  public CompletableFuture<String> greeting(String string);

  @ServiceMethod
  public CompletableFuture<StreamMessage> greetingMessage(StreamMessage request);
  
}
