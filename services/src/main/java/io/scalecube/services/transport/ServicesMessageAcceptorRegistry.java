package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServicesMessageAcceptorRegistry implements ServerMessageAcceptor {

  private ConcurrentMap<String, ServiceMethodInvoker<ServiceMessage>> handlers = new ConcurrentHashMap<>();

  
  public void register(final String qualifier, ServiceMethodInvoker<ServiceMessage> handler) {
    handlers.put(qualifier, handler);
  }
  
  public ServicesMessageAcceptorRegistry() {
    //
  }

  @Override
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    ServiceMethodInvoker<Publisher<ServiceMessage>> handler = null;
    return handler.invoke(request);
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
