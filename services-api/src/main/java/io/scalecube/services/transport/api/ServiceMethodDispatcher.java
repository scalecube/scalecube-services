package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceMethodDispatcher {

  Class requestType();

  Class returnType();

  Mono<ServiceMessage> requestResponse(ServiceMessage request);

  Flux<ServiceMessage> requestStream(ServiceMessage request);

  Mono<Void> fireAndForget(ServiceMessage request);

  Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request);
}
