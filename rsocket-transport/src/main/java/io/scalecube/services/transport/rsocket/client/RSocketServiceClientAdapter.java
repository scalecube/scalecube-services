package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.Payload;
import io.rsocket.RSocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private RSocket rSocket;
  private ServiceMessageCodec<Payload> codec;

  public RSocketServiceClientAdapter(RSocket rSocket, ServiceMessageCodec<Payload> codec) {
    this.rSocket = rSocket;
    this.codec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return rSocket.requestResponse(codec.encodeMessage(request)).map(codec::decodeMessage);
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return rSocket.requestStream(codec.encodeMessage(request)).map(codec::decodeMessage);
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return rSocket.fireAndForget(codec.encodeMessage(request));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return rSocket.requestChannel(request.map(codec::encodeMessage)).map(codec::decodeMessage);
  }

}
