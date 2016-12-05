package io.scalecube.services.a.b.testing;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;


@Service
public interface CanaryService {

  @ServiceMethod
  CompletableFuture<String> greeting(String string);

}
