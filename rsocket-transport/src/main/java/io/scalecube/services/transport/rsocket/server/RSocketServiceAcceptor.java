package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.transport.server.api.ServiceMessageAcceptor;

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
  private final ServiceMessageAcceptor acceptor;

  public RSocketServiceAcceptor(ServiceMessageAcceptor acceptor, ServiceMessageCodec codec) {
    this.messageCodec = codec;
    this.acceptor = acceptor;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {

    return Mono.just(new AbstractRSocket() {
      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        // FIXME: need to seek handler and invoke it.
        throw new UnsupportedOperationException("requestChannel");
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        try {
          return acceptor.requestStream(toMessage(payload))
              .onErrorResume(t -> Flux.just(ExceptionProcessor.toMessage(t)))
              .map(this::toPayload);
        } catch (Throwable ex) {
          return Flux.just(toPayload(ExceptionProcessor.toMessage(ex)));
        }
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        try {
          return acceptor.requestResponse(toMessage(payload))
              .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)))
              .map(this::toPayload);
        } catch (Throwable ex) {
          return Mono.just(toPayload(ExceptionProcessor.toMessage(ex)));
        }
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        try {
          return acceptor.fireAndForget(toMessage(payload)).onErrorResume(Mono::error);
        } catch (Throwable ex) {
          return Mono.error(ex);
        }
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
