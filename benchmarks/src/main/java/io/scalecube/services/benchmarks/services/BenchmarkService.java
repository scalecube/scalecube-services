package io.scalecube.services.benchmarks.services;

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

  @ServiceMethod
  Flux<String> requestManyNoParams();

  @ServiceMethod
  Flux<String> requestBidirectionalEcho(Flux<String> counts);

  @ServiceMethod
  Flux<Long> nanoTime(int count);
}
