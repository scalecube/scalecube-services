package io.scalecube.services.transport.server.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServerMessageAcceptor {

  Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> payloads);

  Flux<ServiceMessage> requestStream(ServiceMessage payload);

  Mono<ServiceMessage> requestResponse(ServiceMessage payload);

  Mono<ServiceMessage> fireAndForget(ServiceMessage payload);

}
