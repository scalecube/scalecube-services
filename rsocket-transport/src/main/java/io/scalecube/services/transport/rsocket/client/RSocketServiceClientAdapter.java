package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.Payload;
import io.rsocket.RSocket;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private Publisher<RSocket> rSocket;
  private ServiceMessageCodec<Payload> codec;

  public RSocketServiceClientAdapter(Publisher<RSocket> rSocket, ServiceMessageCodec<Payload> codec) {
    this.rSocket = rSocket;
    this.codec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return Mono.from(rSocket).flatMap(rSocket -> {
      Payload payload = codec.encodeMessage(request);
      return rSocket.requestResponse(payload).map(codec::decodeMessage);
    });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.from(rSocket).flatMap(rSocket -> {
      Payload payload = codec.encodeMessage(request);
      return rSocket.requestStream(payload).map(codec::decodeMessage);
    });
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(rSocket).flatMap(rSocket -> {
      Payload payload = codec.encodeMessage(request);
      return rSocket.fireAndForget(payload);
    });
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return Flux.from(rSocket).flatMap(rSocket -> {
      Flux<Payload> payloadFlux = request.map(codec::encodeMessage);
      return rSocket.requestChannel(payloadFlux).map(codec::decodeMessage);
    });
  }

}
