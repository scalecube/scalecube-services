package io.scalecube.services.benchmarks;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface BenchmarkService {

  @ServiceMethod
  Mono<Void> oneWay(String request);

  @ServiceMethod
  Mono<String> requestOne(String request);

  @ServiceMethod
  Flux<String> requestMany(int count);
}
