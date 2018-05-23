package io.scalecube.services.benchmarks;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Mono;

@Service
public interface BenchmarkService {

  @ServiceMethod
  Mono<Void> fireAndForget(BenchmarkMessage request);
}
