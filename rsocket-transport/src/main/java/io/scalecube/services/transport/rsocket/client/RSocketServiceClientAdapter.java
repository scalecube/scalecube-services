package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private Publisher<RSocket> rSocket;
  private ServiceMessageCodec messageCodec;

  public RSocketServiceClientAdapter(Publisher<RSocket> rSocket, ServiceMessageCodec codec) {
    this.rSocket = rSocket;
    this.messageCodec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return Mono.from(rSocket)
        .flatMap(rSocket -> rSocket.requestResponse(messageCodec.encodeAndTransform(request, ByteBufPayload::create))
            .map(payload1 -> messageCodec.decode(payload1.sliceData(), payload1.sliceMetadata())));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.from(rSocket)
        .flatMap(rSocket -> rSocket.requestStream(messageCodec.encodeAndTransform(request, ByteBufPayload::create))
            .map(payload1 -> messageCodec.decode(payload1.sliceData(), payload1.sliceMetadata())));
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(rSocket)
        .flatMap(rSocket -> rSocket.fireAndForget(messageCodec.encodeAndTransform(request, ByteBufPayload::create)));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return Flux.from(rSocket)
        .flatMap(rSocket -> rSocket
            .requestChannel(request.map(message -> messageCodec.encodeAndTransform(message, ByteBufPayload::create)))
            .map(payload1 -> messageCodec.decode(payload1.sliceData(), payload1.sliceMetadata())));
  }

}
