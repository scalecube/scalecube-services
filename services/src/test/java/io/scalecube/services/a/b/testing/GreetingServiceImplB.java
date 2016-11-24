package io.scalecube.services.a.b.testing;

import io.scalecube.services.GreetingRequest;
import io.scalecube.services.GreetingResponse;
import io.scalecube.services.GreetingService;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

public final class GreetingServiceImplB implements GreetingService {

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture("B - hello to: " + name);
  }

  @Override
  public CompletableFuture<String> greetingNoParams() {
    return CompletableFuture.completedFuture("hello unknown");
  }

  @Override
  public CompletableFuture<Message> greetingMessage(Message request) {
    return CompletableFuture.completedFuture(Message.fromData(" hello to: " + request.data()));
  }

  @Override
  public CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletableFuture<GreetingResponse> greetingRequest(GreetingRequest string) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void greetingVoid(GreetingRequest request) {
    // TODO Auto-generated method stub

  }

}
