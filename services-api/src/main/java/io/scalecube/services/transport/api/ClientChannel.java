package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import java.lang.reflect.Type;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Client channel interface. */
public interface ClientChannel {

  Mono<ServiceMessage> requestResponse(ServiceMessage message, Type responseType);

  Flux<ServiceMessage> requestStream(ServiceMessage message, Type responseType);

  Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher, Type responseType);
}
