package io.scalecube.services.transport.server.api;

import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

public interface ServerMessageAcceptor {

  public Publisher<ServiceMessage> requestChannel(Publisher<ServiceMessage> payloads);

  public Publisher<ServiceMessage> requestStream(ServiceMessage payload) ;

  public Publisher<ServiceMessage> requestResponse(ServiceMessage payload) ;

  public Publisher<Void> fireAndForget(ServiceMessage payload);
  
}
