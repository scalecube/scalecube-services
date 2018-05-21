package io.scalecube.services.api;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public interface ServiceMessageHandler {

  Flux<ServiceMessage> invoke(Publisher<ServiceMessage> publisher);
}
