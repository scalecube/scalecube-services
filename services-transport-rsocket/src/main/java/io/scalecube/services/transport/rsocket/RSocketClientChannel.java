package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.lang.reflect.Type;
import java.nio.channels.ClosedChannelException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketClientChannel implements ClientChannel {

  private Mono<RSocket> rsocket;
  private ServiceMessageCodec messageCodec;

  public RSocketClientChannel(Mono<RSocket> rsocket, ServiceMessageCodec codec) {
    this.rsocket = rsocket;
    this.messageCodec = codec;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage message, Type responseType) {
    return rsocket
        .flatMap(
            rsocket ->
                rsocket
                    .requestResponse(toPayload(message))
                    .onErrorMap(
                        ClosedChannelException.class,
                        e -> new ConnectionClosedException("Connection closed")))
        .map(this::toMessage)
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message, Type responseType) {
    return rsocket
        .flatMapMany(
            rsocket ->
                rsocket
                    .requestStream(toPayload(message))
                    .onErrorMap(
                        ClosedChannelException.class,
                        e -> new ConnectionClosedException("Connection closed")))
        .map(this::toMessage)
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(
      Publisher<ServiceMessage> publisher, Type responseType) {
    return rsocket
        .flatMapMany(
            rsocket ->
                rsocket
                    .requestChannel(Flux.from(publisher).map(this::toPayload))
                    .onErrorMap(
                        ClosedChannelException.class,
                        e -> new ConnectionClosedException("Connection closed")))
        .map(this::toMessage)
        .map(msg -> ServiceMessageCodec.decodeData(msg, responseType));
  }

  private Payload toPayload(ServiceMessage request) {
    return messageCodec.encodeAndTransform(request, ByteBufPayload::create);
  }

  private ServiceMessage toMessage(Payload payload) {
    return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
  }
}
