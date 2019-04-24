package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Client channel interface. */
public interface ClientChannel {

  default Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    return Mono.error(new IllegalStateException("no client channel implemenation"));
  }

  default Flux<ServiceMessage> requestStream(ServiceMessage message) {
    return Flux.error(new IllegalStateException("no client channel implemenation"));
  }

  default Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return Flux.error(new IllegalStateException("no client channel implemenation"));
  }
}
