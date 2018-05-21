package io.scalecube.gateway.websocket;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Mono;

public class WebsocketGateway2 {

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

          EmitterProcessor<ServiceMessage> requestEmitter = EmitterProcessor.create();
          EmitterProcessor<ServiceMessage> responseEmitter = EmitterProcessor.create();

          session.receive().log(">>> recv").subscribe(requestEmitter::onNext);

          defineInvocation(requestEmitter, responseEmitter);

          return session.send(responseEmitter.log("<<< send"));
        }

        private void defineInvocation(EmitterProcessor<ServiceMessage> requestEmitter,
            EmitterProcessor<ServiceMessage> responseEmitter) {
          call.requestBidirectional(requestEmitter)
              .doOnError(throwable -> defineInvocation(requestEmitter, responseEmitter))
              .onErrorResume(throwable -> Mono.just(ExceptionProcessor.toMessage(throwable)))
              .map(dataCodec::encode)
              .subscribe(responseEmitter::onNext);
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
