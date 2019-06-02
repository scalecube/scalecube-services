package io.scalecube.services.gateway.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.GatewayMetrics;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketGatewayAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketGatewayAcceptor.class);

  private final ServiceCall serviceCall;
  private final GatewayMetrics metrics;

  public RSocketGatewayAcceptor(ServiceCall serviceCall, GatewayMetrics metrics) {
    this.serviceCall = serviceCall;
    this.metrics = metrics;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rsocket) {
    LOGGER.info("Accepted rsocket websocket: {}, connectionSetup: {}", rsocket, setup);

    rsocket
        .onClose()
        .doOnTerminate(() -> LOGGER.info("Client disconnected: {}", rsocket))
        .subscribe(null, th -> LOGGER.error("Exception on closing rsocket: {}", th));

    // Prepare message codec together with headers from metainfo
    HeadersCodec headersCodec = HeadersCodec.getInstance(setup.metadataMimeType());
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    return Mono.just(new GatewayRSocket(serviceCall, metrics, messageCodec));
  }

  /**
   * Private extension class for rsocket. Holds gateway business logic in following methods: {@link
   * #fireAndForget(Payload)}, {@link #requestResponse(Payload)}, {@link #requestStream(Payload)}
   * and {@link #requestChannel(org.reactivestreams.Publisher)}.
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
                .doOnNext(payload1 -> metrics.markServiceResponse());
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
                .doOnNext(payload1 -> metrics.markServiceResponse());
          });
    }

    private ServiceMessage toMessage(Payload payload) {
      return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
    }

    private Payload toPayload(ServiceMessage message) {
      return messageCodec.encodeAndTransform(message, ByteBufPayload::create);
    }
  }
}
