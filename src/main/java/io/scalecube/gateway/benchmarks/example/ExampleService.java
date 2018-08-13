package io.scalecube.gateway.benchmarks.example;

import io.scalecube.gateway.examples.StreamRequest;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("greeting")
public interface ExampleService {

  @ServiceMethod("one")
  Mono<String> one(String name);

  @ServiceMethod("manyStream")
  Flux<Long> manyStream(Long cnt);

  @ServiceMethod("manyStreamWithBackpressureDrop")
  Flux<Long> manyStreamWithBackpressureDrop(Long cnt);

  @ServiceMethod("requestInfiniteStream")
  Flux<Long> requestInfiniteStream(StreamRequest request);
}
