package io.scalecube.examples.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

@Service
public interface GreetingService {

  @ServiceMethod
  String syncGreeting(String name);

  @ServiceMethod
  CompletableFuture<String> asyncGreeting(String string);

  @ServiceMethod
  String syncGreetingException(String name);

  @ServiceMethod
  CompletableFuture<String> asyncGreetingException(String name);

}
