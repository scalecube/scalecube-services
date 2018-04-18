package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.transport.rsocket.PayloadCodec;
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

  private PayloadCodec payloadCodec;
  private ServerMessageAcceptor acceptor;
 
  public RSocketServiceMethodAcceptor(ServerMessageAcceptor acceptor, PayloadCodec payloadCodec) {
   this.payloadCodec = payloadCodec;
   this.acceptor = acceptor;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    
    return Mono.just(new AbstractRSocket() {
      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return null;  
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        return acceptor.requestStream(payloadCodec.decode(payload))
            .map(response->payloadCodec.encode(response));
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        return acceptor.requestResponse(payloadCodec.decode(payload))
            .map(response->payloadCodec.encode(response));
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return acceptor.fireAndForget(payloadCodec.decode(payload));
      }
    });
  }
}
