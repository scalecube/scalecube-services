package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;


@Service
public interface StringUnaryOperator {

  @ServiceMethod
  public CompletableFuture<String> apply(String t);
};
