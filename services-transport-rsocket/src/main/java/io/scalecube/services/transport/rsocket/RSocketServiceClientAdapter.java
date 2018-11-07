package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceClientAdapter implements ClientChannel {

  private Mono<RSocket> rsocket;
  private ServiceMessageCodec messageCodec;

  public RSocketServiceClientAdapter(Mono<RSocket> rsocket, ServiceMessageCodec codec) {
    this.rsocket = rsocket;
    this.messageCodec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    return rsocket
        .flatMap(
            rsocket ->
                rsocket
                    .requestResponse(toPayload(message))
                    .takeUntilOther(listenConnectionClose(rsocket)))
        .map(this::toMessage);
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    return rsocket
        .flatMapMany(
            rsocket ->
                rsocket
                    .requestStream(toPayload(message))
                    .takeUntilOther(listenConnectionClose(rsocket)))
        .map(this::toMessage);
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return rsocket
        .flatMapMany(
            rsocket ->
                rsocket
                    .requestChannel(Flux.from(publisher).map(this::toPayload))
                    .takeUntilOther(listenConnectionClose(rsocket)))
        .map(this::toMessage);
  }

  private Payload toPayload(ServiceMessage request) {
    return messageCodec.encodeAndTransform(request, ByteBufPayload::create);
  }

  private ServiceMessage toMessage(Payload payload) {
    return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
  }

  @SuppressWarnings("unchecked")
  private <T> Mono<T> listenConnectionClose(RSocket rsocket) {
    return rsocket
        .onClose()
        .map(empty -> (T) empty)
        .switchIfEmpty(Mono.defer(this::toConnectionClosedException));
  }

  private <T> Mono<T> toConnectionClosedException() {
    return Mono.error(new ConnectionClosedException("Connection closed"));
  }
}
