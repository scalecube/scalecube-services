package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public final class DefaultServiceMessageAcceptor implements ServiceMessageHandler {

  private final LocalServiceHandlers serviceHandlers;

  public DefaultServiceMessageAcceptor(LocalServiceHandlers serviceHandlers) {
    this.serviceHandlers = serviceHandlers;
  }

  @Override
  public Flux<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    return Flux.from(HeadAndTail.createFrom(publisher)).flatMap(pair -> {
      ServiceMessage message = pair.head();
      ServiceMessageHandler dispatcher = serviceHandlers.get(message.qualifier());
      return dispatcher.invoke(Flux.from(pair.tail()).startWith(message));
    });
  }
}
