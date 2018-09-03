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

  String SERVICE_RECV_TIME = "service-recv-time";
  String CLIENT_RECV_TIME = "client-recv-time";
  String GW_RECV_FROM_SERVICE_TIME = "gw-recv-from-service-time";
  String CLIENT_SEND_TIME = "client-send-time";
  String GW_RECV_FROM_CLIENT_TIME = "gw-recv-from-client-time";

  @ServiceMethod
  Mono<ServiceMessage> one(ServiceMessage message);

  @ServiceMethod
  Mono<ServiceMessage> failure(ServiceMessage message);

  @ServiceMethod
  Flux<ServiceMessage> broadcastStream(ServiceMessage message);

  @ServiceMethod
  Flux<ServiceMessage> infiniteStream(ServiceMessage message);
}
