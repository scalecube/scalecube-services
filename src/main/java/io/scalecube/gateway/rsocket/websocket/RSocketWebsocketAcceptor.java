package io.scalecube.gateway.rsocket.websocket;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.metrics.Metrics;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketWebsocketAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketAcceptor.class);
  public static final String METRICS_PREFIX = "rsocket";
  public static final String METRIC_STREAM_DURATION = "streamDurationTimer";
  public static final String METRIC_CLIENT_CONNECTIONS = "client.connections";
  public static final String METRIC_CLIENT = "client";
  public static final String METRIC_REQUESTS = "requests";
  public static final String METRIC_RESPONSES = "responses";

  private ServiceCall serviceCall;
  private final Metrics metrics;

  public RSocketWebsocketAcceptor(ServiceCall serviceCall, Metrics metrics) {
    this.serviceCall = serviceCall;
    this.metrics = metrics != null ? metrics : new Metrics(new MetricRegistry());
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rSocket) {
    LOGGER.info("Accepted rSocket websocket: {}, connectionSetup: {}", rSocket, setup);

    rSocket
      .onClose()
      .doOnTerminate(
        () -> {
          LOGGER.info("Client disconnected: {}", rSocket);
          metrics.getCounter(METRICS_PREFIX, METRIC_CLIENT_CONNECTIONS).dec();
        })
      .subscribe();

    HeadersCodec headersCodec = HeadersCodec.getInstance(setup.metadataMimeType());

    ServiceMessageCodec codec = new ServiceMessageCodec(headersCodec);
    metrics.getCounter(METRICS_PREFIX, METRIC_CLIENT_CONNECTIONS).inc();
    return Mono.just(
      new AbstractRSocket() {
        @Override
        public Mono<Void> fireAndForget(Payload payload) {
          metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_REQUESTS).mark();
          Timer.Context roundtripTime =
            metrics.getTimer(METRICS_PREFIX, METRIC_STREAM_DURATION).time();
          return serviceCall.oneWay(toServiceMessage(payload)).doOnTerminate(roundtripTime::stop);
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
          metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_REQUESTS).mark();
          Timer.Context streamDuration =
            RSocketWebsocketAcceptor.this
              .metrics
              .getTimer(METRICS_PREFIX, METRIC_STREAM_DURATION)
              .time();
          return serviceCall
            .requestOne(toServiceMessage(payload))
            .map(this::toPayload)
            .doOnNext(
              n -> metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_RESPONSES).mark())
            .doOnTerminate(streamDuration::stop);
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
          metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_REQUESTS).mark();
          Timer.Context streamDuration =
            RSocketWebsocketAcceptor.this
              .metrics
              .getTimer(METRICS_PREFIX, METRIC_STREAM_DURATION)
              .time();
          return serviceCall
            .requestMany(toServiceMessage(payload))
            .map(this::toPayload)
            .doOnNext(
              n -> metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_RESPONSES).mark())
            .doOnTerminate(streamDuration::stop);
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
          Timer.Context streamDuration =
            RSocketWebsocketAcceptor.this
              .metrics
              .getTimer(METRICS_PREFIX, METRIC_STREAM_DURATION)
              .time();
          final Publisher<ServiceMessage> publisher =
            Flux.from(payloads)
              .doOnNext(
                n ->
                  metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_REQUESTS).mark())
              .map(this::toServiceMessage);
          return serviceCall
            .requestBidirectional(publisher)
            .map(this::toPayload)
            .doOnNext(
              n -> metrics.getMeter(METRICS_PREFIX, METRIC_CLIENT, METRIC_RESPONSES).mark())
            .doOnTerminate(streamDuration::stop);
        }

        private ServiceMessage toServiceMessage(Payload payload) {
          return codec.decode(payload.sliceData(), payload.sliceMetadata());
        }

        private Payload toPayload(ServiceMessage serviceMessage) {
          return codec.encodeAndTransform(serviceMessage, ByteBufPayload::create);
        }
      });
  }
}
