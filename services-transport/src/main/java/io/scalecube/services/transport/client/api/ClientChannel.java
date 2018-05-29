package io.scalecube.services.transport.client.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public interface ClientChannel {

  Flux<ServiceMessage> requestStream(ServiceMessage message);

  Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher);
}
