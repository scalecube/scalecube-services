package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServicesMessageAcceptor implements ServerMessageAcceptor {

  private Map<String, ServiceMethodInvoker<ServiceMessage>> handlers;

  public DefaultServicesMessageAcceptor(Map<String, ServiceMethodInvoker<ServiceMessage>> services) {
    this.handlers = services;
  }

  @Override
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    Flux<ServiceMethodInvoker<ServiceMessage>> stream = Flux.from(request).map(mapper->handlers.get(mapper));
    
    return null;
  }


  @Override
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    return handlers.get(request.qualifier()).invoke(request);
  }

  @Override
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    return handlers.get(request.qualifier())
        .invoke(request);
  }

  @Override
  public Publisher<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(handlers.get(request.qualifier())
        .invoke(request))
        .map(msg->null);
  }

}
