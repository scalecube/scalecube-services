package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;

import reactor.core.publisher.Flux;

public final class DefaultServiceMessageAcceptor implements ServiceMessageHandler {

  private final LocalServiceHandlers serviceHandlers;

  public DefaultServiceMessageAcceptor(LocalServiceHandlers serviceHandlers) {
    this.serviceHandlers = serviceHandlers;
  }

  @Override
  public Flux<ServiceMessage> invoke(Flux<ServiceMessage> publisher) {
    return Flux.from(HeadAndTail.createFrom(publisher)).flatMap(pair -> {
      ServiceMessage message = pair.head();
      ServiceMessageHandler dispatcher = serviceHandlers.get(message.qualifier());
      return dispatcher.invoke(Flux.from(pair.tail()).startWith(message));
    });
  }
}
