package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.Payload;
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
        .flatMap(rSocket -> rSocket.requestResponse(toPayload(request)).map(this::toMessage));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.from(rSocket)
        .flatMap(rSocket -> rSocket.requestStream(toPayload(request)).map(this::toMessage));
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(rSocket)
        .flatMap(rSocket -> rSocket.fireAndForget(toPayload(request)));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return Flux.from(rSocket)
        .flatMap(rSocket -> rSocket.requestChannel(request.map(this::toPayload)).map(this::toMessage));
  }

  private Payload toPayload(ServiceMessage request) {
    return messageCodec.encodeAndTransform(request, ByteBufPayload::create);
  }

  private ServiceMessage toMessage(Payload payload1) {
    return messageCodec.decode(payload1.sliceData(), payload1.sliceMetadata());
  }
}
