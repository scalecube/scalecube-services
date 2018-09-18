package io.scalecube.services.benchmarks.service;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("benchmarks")
public interface BenchmarkService {

  @ServiceMethod
  Mono<Void> oneWay(ServiceMessage request);

  @ServiceMethod
  Mono<ServiceMessage> requestOne(ServiceMessage request);

  @ServiceMethod
  Flux<ServiceMessage> requestStreamRange(int count);
}
