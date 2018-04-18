package io.scalecube.services.transport.server.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServerMessageAcceptor {

  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> payloads);

  public Flux<ServiceMessage> requestStream(ServiceMessage payload) ;

  public Mono<ServiceMessage> requestResponse(ServiceMessage payload) ;

  public Mono<Void> fireAndForget(ServiceMessage payload);
  
}
