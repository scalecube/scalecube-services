package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.codec.ServiceMessageCodec;
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
        throw new UnsupportedOperationException();
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        return Flux.from(acceptor.requestStream(messageCodec.decode(payload.sliceData(), payload.sliceMetadata())))
            .map(response -> messageCodec.encodeAndTransform(response, ByteBufPayload::create));
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        return Mono.from(acceptor.requestResponse(messageCodec.decode(payload.sliceData(), payload.sliceMetadata())))
            .map(response -> messageCodec.encodeAndTransform(response, ByteBufPayload::create));
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return Mono.from(acceptor.fireAndForget(messageCodec.decode(payload.sliceData(), payload.sliceMetadata())))
            .map(message -> null);
      }
    });
  }
}
