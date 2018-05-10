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
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher)
        .doOnNext(request -> {

        })
        .transform(flux -> flux.map(request -> {
          ServiceMessageHandler dispatcher = serviceHandlers.get(request.qualifier());
          return dispatcher.invoke(flux);
        })).flatMap(p -> p);
  }
}
