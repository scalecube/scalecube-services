package io.scalecube.services.api;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceMessageHandler {

  Mono<ServiceMessage> requestResponse(ServiceMessage message);

  Flux<ServiceMessage> requestStream(ServiceMessage message);

  Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher);
}
