package io.scalecube.services.transport.rsocket.server;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public class RSocketServiceMethodAcceptor implements SocketAcceptor {

  private Collection<ServiceInstance> services;
  private PayloadCodec payloadCodec;

  public RSocketServiceMethodAcceptor(PayloadCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
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
