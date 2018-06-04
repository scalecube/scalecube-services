package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class DefaultServiceMessageAcceptor implements ServiceMessageHandler {

  private final LocalServiceHandlers serviceHandlers;

  public DefaultServiceMessageAcceptor(LocalServiceHandlers serviceHandlers) {
    this.serviceHandlers = serviceHandlers;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    return serviceHandlers.get(message.qualifier()).requestResponse(message);
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    return serviceHandlers.get(message.qualifier()).requestStream(message);
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return Flux.from(HeadAndTail.createFrom(publisher)).flatMap(pair -> {
      ServiceMessage message = pair.head();
      ServiceMessageHandler dispatcher = serviceHandlers.get(message.qualifier());
      return dispatcher.requestChannel(Flux.from(pair.tail()).startWith(message));
    });
  }
}
