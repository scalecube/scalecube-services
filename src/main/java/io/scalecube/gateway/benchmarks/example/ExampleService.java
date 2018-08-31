package io.scalecube.gateway.benchmarks.example;

import io.scalecube.gateway.examples.StreamRequest;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(ExampleService.QUALIFIER)
public interface ExampleService {

  String QUALIFIER = "greeting";

  @ServiceMethod("one")
  Mono<String> one(String name);

  @ServiceMethod("oneMessage")
  Mono<ServiceMessage> oneMessage(ServiceMessage request);

  @ServiceMethod("manyStream")
  Flux<Long> manyStream(Long cnt);

  @ServiceMethod("manyStreamWithBackpressureDrop")
  Flux<Long> manyStreamWithBackpressureDrop(Long cnt);

  @ServiceMethod("requestInfiniteStream")
  Flux<Long> requestInfiniteStream(StreamRequest request);

  @ServiceMethod("requestInfiniteMessageStream")
  Flux<ServiceMessage> requestInfiniteMessageStream(ServiceMessage request);
}
