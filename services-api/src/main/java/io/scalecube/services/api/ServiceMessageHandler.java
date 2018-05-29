package io.scalecube.services.api;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public interface ServiceMessageHandler {

  Flux<ServiceMessage> requestStream(ServiceMessage message);

  Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher);
}
