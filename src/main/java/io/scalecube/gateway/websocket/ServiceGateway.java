package io.scalecube.gateway.websocket;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.codec.ServiceMessageDataCodec;

import reactor.core.publisher.Mono;

public class ServiceGateway {

  public static class Builder {

    private Call call;
    private WebSocketAcceptor acceptor;
    private ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

    public Builder(Call call) {
      this.call = call;
    }

    public Builder ws() {
      acceptor = new WebSocketAcceptor() {
        @Override
        public Mono<Void> onConnect(WebSocketSession session) {
          return session.send(
              call.invoke(session.receive()
                  .log("recv"))
              .log("send")
              .map(dataCodec::encode)).then();
        }

        @Override
        public Mono<Void> onDisconnect(WebSocketSession session) {
          return Mono.never();
        }
      }; 
      return this;
    }

    public WebSocketServer build() {
      return new WebSocketServer(acceptor);
    }

  }

  public static Builder builder(Call call) {
    return new Builder(call);
  }

}
