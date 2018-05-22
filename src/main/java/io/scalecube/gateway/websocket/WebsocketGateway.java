package io.scalecube.gateway.websocket;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

public class WebsocketGateway {

  private WebSocketServer webSocketServer;
  private Flux<ServiceMessage> inbound;

  public WebsocketGateway(Builder builder) {
    this.webSocketServer = builder.webSocketServer;
    this.inbound = builder.inbound;
  }

  public Flux<ServiceMessage> receive() {
    return inbound;
  }

  public InetSocketAddress start(InetSocketAddress inetSocketAddress) {
    return webSocketServer.start(inetSocketAddress);
  }


  public static class Builder {

    private ServiceCall call;
    private WebSocketAcceptor acceptor;
    private ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();
    private Flux<ServiceMessage> inbound;
    private WebSocketServer webSocketServer;

    public Builder(ServiceCall call) {
      this.call = call;
    }

    public Builder dataCodec(ServiceMessageDataCodec dataCodec) {
      this.dataCodec = dataCodec;
      return this;
    }

    public WebsocketGateway build() {
      acceptor = new WebSocketAcceptor() {
        @Override
        public Mono<Void> onConnect(WebSocketSession session) {
          return session.send(session.receive().flatMap(message -> call.requestBidirectional(Mono.just(message))
              .onErrorResume(throwable -> Mono.just(ExceptionProcessor.toMessage(throwable)))
              .map(dataCodec::encode)));
        }

        @Override
        public Mono<Void> onDisconnect(WebSocketSession session) {
          return Mono.never();
        }
      };
      this.webSocketServer = new WebSocketServer(acceptor);
      return new WebsocketGateway(this);
    }

  }

  public static Builder builder(ServiceCall call) {
    return new Builder(call);
  }
}
