package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

import java.nio.charset.Charset;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private Mono<RSocket> rSocket;
  private ServiceMessageCodec messageCodec;

  public RSocketServiceClientAdapter(Mono<RSocket> rSocket, ServiceMessageCodec codec) {
    this.rSocket = rSocket;
    this.messageCodec = codec;
  }

  @Override
  public Flux<ServiceMessage> requestBidirectional(Flux<ServiceMessage> publisher) {
    return rSocket.as(Flux::from).flatMap(rSocket -> {
      Flux<Payload> payloads = publisher.map(this::toPayload);
      return rSocket.requestChannel(payloads).map(this::toMessage);
    });
  }

  private Payload toPayload(ServiceMessage request) {
    Payload payload = messageCodec.encodeAndTransform(request, ByteBufPayload::create);
    System.err.println(">>> Client encode: " + payload.sliceMetadata().toString(Charset.defaultCharset()));
    return payload;
  }

  private ServiceMessage toMessage(Payload payload) {
    System.err.println(">>> Client decode: " + payload.sliceMetadata().toString(Charset.defaultCharset()));
    return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
  }
}
