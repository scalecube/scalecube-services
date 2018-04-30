package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private Publisher<RSocket> rSocket;
  private ServiceMessageCodec codec;

  public RSocketServiceClientAdapter(Publisher<RSocket> rSocket, ServiceMessageCodec codec) {
    this.rSocket = rSocket;
    this.codec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return Mono.from(rSocket).flatMap(rSocket -> {
      ByteBuf[] bufs = codec.encodeMessage(request);
      Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);
      return rSocket.requestResponse(payload)
          .map(payload1 -> codec.decodeMessage(payload1.sliceData(), payload1.sliceMetadata()));
    });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.from(rSocket).flatMap(rSocket -> {
      ByteBuf[] bufs = codec.encodeMessage(request);
      Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);
      return rSocket.requestStream(payload)
          .map(payload1 -> codec.decodeMessage(payload1.sliceData(), payload1.sliceMetadata()));
    });
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(rSocket).flatMap(rSocket -> {
      ByteBuf[] bufs = codec.encodeMessage(request);
      Payload payload = ByteBufPayload.create(bufs[0], bufs[1]);
      return rSocket.fireAndForget(payload);
    });
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return Flux.from(rSocket).flatMap(rSocket -> {
      Flux<Payload> payloadFlux = request.map(message -> {
        ByteBuf[] bufs = codec.encodeMessage(message);
        return ByteBufPayload.create(bufs[0], bufs[1]);
      });
      return rSocket.requestChannel(payloadFlux)
          .map(payload1 -> codec.decodeMessage(payload1.sliceData(), payload1.sliceMetadata()));
    });
  }

}
