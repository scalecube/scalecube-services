package io.scalecube.services.api;

import reactor.core.publisher.Flux;

public interface ServiceMessageHandler {

  Flux<ServiceMessage> invoke(Flux<ServiceMessage> publisher);
}
