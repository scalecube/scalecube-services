package io.scalecube.services.benchmarks;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Mono;

@Service
public interface BenchmarkService {

  @ServiceMethod
  Mono<Void> fireAndForget0();

  @ServiceMethod
  Mono<Void> fireAndForget(BenchmarkMessage request);

  @ServiceMethod
  Mono<BenchmarkMessage> requestOne(BenchmarkMessage request);
}
