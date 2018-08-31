package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.NAMESPACE;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(NAMESPACE)
public interface BenchmarksService {

  String NAMESPACE = "benchmarks";
  String TIMESTAMP_KEY = "timestamp";
  String MESSAGES_PER_INTERVAL = "messagesPerInterval";
  String INTERVAL_MILLIS = "intervalMillis";

  @ServiceMethod
  Mono<ServiceMessage> one(ServiceMessage message);

  @ServiceMethod
  Mono<ServiceMessage> failure(ServiceMessage message);

  @ServiceMethod
  Flux<ServiceMessage> broadcastStream();

  @ServiceMethod
  Flux<ServiceMessage> infiniteStream();

  @ServiceMethod
  Flux<ServiceMessage> infiniteStreamWithRate(ServiceMessage message);
}
