package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceAcceptor implements SocketAcceptor {

  private final ServiceMessageCodec messageCodec;
  private final ServiceMessageHandler acceptor;

  public RSocketServiceAcceptor(ServiceMessageHandler acceptor, ServiceMessageCodec codec) {
    this.messageCodec = codec;
    this.acceptor = acceptor;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket socket) {
    return Mono.just(new AbstractRSocket() {
      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return acceptor.invoke(Flux.from(payloads).map(this::toMessage))
            .onErrorResume(t -> Flux.just(ExceptionProcessor.toMessage(t)))
            .map(this::toPayload);
      }

      private Payload toPayload(ServiceMessage response) {
        return messageCodec.encodeAndTransform(response, ByteBufPayload::create);
      }

      private ServiceMessage toMessage(Payload payload) {
        return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
      }
    });
  }
}
