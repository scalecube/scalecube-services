package io.scalecube.services.transport.client.api;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;

public interface ClientChannel {

  Flux<ServiceMessage> requestBidirectional(Flux<ServiceMessage> publisher);

}
