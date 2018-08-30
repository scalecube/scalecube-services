package io.scalecube.gateway.rsocket.websocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketWebsocketAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketAcceptor.class);

  private final ServiceCall serviceCall;
  private final GatewayMetrics metrics;

  public RSocketWebsocketAcceptor(ServiceCall serviceCall, GatewayMetrics metrics) {
    this.serviceCall = serviceCall;
    this.metrics = metrics;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rsocket) {
    LOGGER.info("Accepted rsocket websocket: {}, connectionSetup: {}", rsocket, setup);
    metrics.incConnection();

    rsocket
        .onClose()
        .doOnTerminate(
            () -> {
              LOGGER.info("Client disconnected: {}", rsocket);
              metrics.decConnection();
            })
        .subscribe();

    // Prepare message codec together with headers from metainfo
    HeadersCodec headersCodec = HeadersCodec.getInstance(setup.metadataMimeType());
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    return Mono.just(new GatewayRSocket(serviceCall, metrics, messageCodec));
  }

  /**
   * Private extension class for rsocket. Holds gateway business logic in following methods: {@link
   * #fireAndForget(Payload)}, {@link #requestResponse(Payload)}, {@link #requestStream(Payload)}
   * and {@link #requestChannel(Publisher)}.
   */
  private static class GatewayRSocket extends AbstractRSocket {

    private final ServiceCall serviceCall;
    private final GatewayMetrics metrics;
    private final ServiceMessageCodec messageCodec;

    /**
     * Constructor for gateway rsocket.
     *
     * @param serviceCall service call coming from microservices.
     * @param metrics gateway metrics.
     * @param messageCodec message messageCodec.
     */
    private GatewayRSocket(
        ServiceCall serviceCall, GatewayMetrics metrics, ServiceMessageCodec messageCodec) {
      this.serviceCall = serviceCall;
      this.metrics = metrics;
      this.messageCodec = messageCodec;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return Mono.defer(
          () -> {
            metrics.markRequest();
            return serviceCall.oneWay(toMessage(payload));
          });
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.defer(
          () -> {
            metrics.markRequest();
            return serviceCall
                .requestOne(toMessage(payload))
                .map(this::toPayload)
                .doOnNext(payload1 -> metrics.markResponse());
          });
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.defer(
          () -> {
            metrics.markRequest();
            return serviceCall
                .requestMany(toMessage(payload))
                .map(this::toPayload)
                .doOnNext(payload1 -> metrics.markResponse());
          });
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.defer(
          () ->
              serviceCall
                  .requestBidirectional(
                      Flux.from(payloads)
                          .doOnNext(payload -> metrics.markRequest())
                          .map(this::toMessage))
                  .map(this::toPayload)
                  .doOnNext(payload -> metrics.markResponse()));
    }

    private ServiceMessage toMessage(Payload payload) {
      ServiceMessage decodedMessage =
          messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
      return ServiceMessage.from(decodedMessage)
          .header("gw-recd-from-client-time", String.valueOf(System.currentTimeMillis()))
          .build();
    }

    private Payload toPayload(ServiceMessage serviceMessage) {
      ServiceMessage message =
          ServiceMessage.from(serviceMessage)
              .header("gw-recd-from-srv-time", String.valueOf(System.currentTimeMillis()))
              .build();
      return messageCodec.encodeAndTransform(message, ByteBufPayload::create);
    }
  }
}
