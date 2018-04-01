package io.scalecube.stresstests.services;

import io.scalecube.streams.StreamMessage;

import java.util.concurrent.CompletableFuture;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public CompletableFuture<String> greeting(String name) {
    return CompletableFuture.completedFuture("hello to: " + name);
  }

  @Override
  public CompletableFuture<StreamMessage> greetingMessage(StreamMessage request) {
    return CompletableFuture.completedFuture(StreamMessage.builder().data("hello to: " + request.data()).build());
  }

}
