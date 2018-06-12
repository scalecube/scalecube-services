package io.scalecube.services.benchmarks;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface BenchmarkService {

  ServiceMessage REQUEST_ONE = ServiceMessage.builder()
      .qualifier(BenchmarkService.class.getName(), "requestOne")
      .data("test")
      .build();

  ServiceMessage ONE_WAY = ServiceMessage.builder()
      .qualifier(BenchmarkService.class.getName(), "oneWay")
      .data("test")
      .build();

  ServiceMessage REQUEST_BIDIRECTIONAL_ECHO = ServiceMessage.builder()
      .qualifier(BenchmarkService.class.getName(), "requestBidirectionalEcho")
      .data("test")
      .build();

  @ServiceMethod
  Mono<Void> oneWay(String request);

  @ServiceMethod
  Mono<String> requestOne(String request);

  @ServiceMethod
  Flux<String> requestMany(int count);

  @ServiceMethod
  Flux<String> requestBidirectionalEcho(Flux<String> counts);

  @ServiceMethod
  Flux<Long> nanoTime(int count);
}
