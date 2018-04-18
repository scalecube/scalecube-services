package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceMethodInvoke {

  Mono<Void> fireAndForget(ServiceMessage request);
  
  Mono<ServiceMessage> requestResponse(ServiceMessage request);

  Flux<ServiceMessage> requestStream(ServiceMessage request) throws Exception;

  Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) throws Exception;
}
