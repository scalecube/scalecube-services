package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.RSocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel{

  private RSocket rSocket;

  public RSocketServiceClientAdapter(RSocket rSocket) {
    this.rSocket= rSocket;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    //rSocket.requestResponse(null);
    return null;
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    // TODO Auto-generated method stub
    return null;
  }

}
