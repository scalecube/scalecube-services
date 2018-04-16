package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.ServiceInstance;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory.Start;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.server.NettyContextCloseable;

import org.reactivestreams.Publisher;

import java.util.Collection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceMethodAcceptor implements SocketAcceptor {

  private Collection<ServiceInstance> services;

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    return Mono.just(new AbstractRSocket() {
      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {

        return null;
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        return null;
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {

        return null;
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return null;
      }
    });
  }
}
