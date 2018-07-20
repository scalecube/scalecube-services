package io.scalecube.gateway.rsocket.websocket;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSocketWebsocketAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketAcceptor.class);

  private ServiceCall serviceCall;

  public RSocketWebsocketAcceptor(ServiceCall serviceCall) {
    this.serviceCall = serviceCall;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rSocket) {
    LOGGER.info("Accepted rSocket websocket: {}, connectionSetup: {}", rSocket, setup);

    HeadersCodec headersCodec = HeadersCodec.getInstance(setup.metadataMimeType());

    ServiceMessageCodec codec = new ServiceMessageCodec(headersCodec);

    return Mono.just(new AbstractRSocket() {
      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return serviceCall.oneWay(toServiceMessage(payload));
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        return serviceCall.requestOne(toServiceMessage(payload)).map(this::toPayload);
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        return serviceCall.requestMany(toServiceMessage(payload)).map(this::toPayload);
      }

      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        final Publisher<ServiceMessage> publisher = Flux.from(payloads).map(this::toServiceMessage);
        return serviceCall.requestBidirectional(publisher).map(this::toPayload);
      }

      private ServiceMessage toServiceMessage(Payload payload) {
        return codec.decode(payload.sliceData(), payload.sliceMetadata());
      }

      private Payload toPayload(ServiceMessage serviceMessage) {
        return codec.encodeAndTransform(serviceMessage, ByteBufPayload::create);
      }

      @Override
      public Mono<Void> onClose() {
        LOGGER.error("------------------> onClose");
        return super.onClose();
      }
    });
  }
}
