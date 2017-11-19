package io.scalecube.services.stress;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

@Service
public interface GreetingService {

  @ServiceMethod
  public CompletableFuture<String> greeting(String string);

  @ServiceMethod
  public CompletableFuture<Message> greetingMessage(Message request);
  
}
