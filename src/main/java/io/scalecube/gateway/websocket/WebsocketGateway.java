package io.scalecube.gateway.websocket;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.codec.ServiceMessageDataCodec;

import reactor.core.publisher.Mono;

public class WebsocketGateway {

  public static class Builder {

    private ServiceCall call;
    private WebSocketAcceptor acceptor;
    private ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

    public Builder(ServiceCall call) {
      this.call = call;
    }

    public WebSocketServer build() {
      acceptor = new WebSocketAcceptor() {
        @Override
        public Mono<Void> onConnect(WebSocketSession session) {
          return session.send(
              call.requestBidirectional(session.receive()
                  .log("recv"))
                  .log("send")
                  .map(dataCodec::encode))
              .then();
        }

        @Override
        public Mono<Void> onDisconnect(WebSocketSession session) {
          return Mono.never();
        }
      };

      return new WebSocketServer(acceptor);
    }

  }

  public static Builder builder(ServiceCall call) {
    return new Builder(call);
  }

}
