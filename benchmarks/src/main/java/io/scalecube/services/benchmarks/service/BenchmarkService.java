package io.scalecube.services.benchmarks.service;

import static io.scalecube.services.benchmarks.service.BenchmarkService.NAMESPACE;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(NAMESPACE)
public interface BenchmarkService {

  String NAMESPACE = "benchmarks";

  String SERVICE_RECV_TIME = "service-recv-time";
  String SERVICE_SEND_TIME = "service-send-time";
  String CLIENT_RECV_TIME = "client-recv-time";
  String CLIENT_SEND_TIME = "client-send-time";

  @ServiceMethod
  Mono<Void> requestVoid(ServiceMessage request);

  @ServiceMethod
  Mono<ServiceMessage> requestOne(ServiceMessage message);

  @ServiceMethod
  Flux<ServiceMessage> infiniteStream(ServiceMessage message);
}
