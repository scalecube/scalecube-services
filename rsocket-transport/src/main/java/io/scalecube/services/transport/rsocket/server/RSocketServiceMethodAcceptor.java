package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceMethodAcceptor implements SocketAcceptor {

  private ServiceMessageCodec<Payload> codec;
  private ServerMessageAcceptor acceptor;

  public RSocketServiceMethodAcceptor(ServerMessageAcceptor acceptor, ServiceMessageCodec<Payload> codec) {
    this.codec = codec;
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
        return Flux.from(acceptor.requestStream(codec.decodeMessage(payload)))
            .map(response -> codec.encodeMessage(response));
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        return Mono.from(acceptor.requestResponse(codec.decodeMessage(payload)))
            .map(response -> codec.encodeMessage(response));
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return Mono.from(acceptor.fireAndForget(codec.decodeMessage(payload)))
            .map(message -> null);
      }
    });
  }
}
