package io.scalecube.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.ReferenceCountUtil;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class GatewayWebsocketAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayWebsocketAcceptor.class);

  private final ServiceCall serviceCall;
  private final GatewayMetrics metrics;
  private final GatewayMessageCodec messageCodec = new GatewayMessageCodec();

  /**
   * Constructor for websocket acceptor.
   *
   * @param serviceCall service call
   * @param metrics metrics instance
   */
  public GatewayWebsocketAcceptor(ServiceCall serviceCall, GatewayMetrics metrics) {
    this.serviceCall = serviceCall;
    this.metrics = metrics;
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    return httpResponse.sendWebsocket(
        (WebsocketInbound inbound, WebsocketOutbound outbound) ->
            onConnect(new WebsocketSession(httpRequest, inbound, outbound)));
  }

  private Mono<Void> onConnect(WebsocketSession session) {
    LOGGER.info("Session connected: " + session);

    Mono<Void> voidMono =
        session.send(
            session
                .receive()
                .doOnNext(input -> metrics.markRequest())
                .flatMap(message -> handleMessage(session, message))
                .doOnNext(input -> metrics.markResponse())
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Unhandled exception occurred: {}, session: {} will be closed",
                            ex,
                            session,
                            ex)));

    session.onClose(() -> LOGGER.info("Session disconnected: " + session));

    return voidMono.then();
  }

  private Flux<ByteBuf> handleMessage(WebsocketSession session, ByteBuf message) {
    return Flux.create(
        sink -> {
          Long sid = null;
          GatewayMessage request = null;
          try {
            request = enrichFromClient(toMessage(message));
            Long streamId = sid = request.streamId();

            // check message contains sid
            checkSidNotNull(streamId, session, request);

            // check session contains sid for CANCEL operation
            if (request.hasSignal(Signal.CANCEL)) {
              handleCancelRequest(sink, streamId, session, request);
              return;
            }

            // check session doesn't contain sid yet
            checkSidNotRegisteredYet(streamId, session, request);

            // check message contains qualifier
            checkQualifierNotNull(session, request);

            AtomicBoolean receivedErrorMessage = new AtomicBoolean(false);

            Flux<ServiceMessage> serviceStream =
                serviceCall
                    .requestMany(GatewayMessage.toServiceMessage(request))
                    .map(this::enrichFromService);

            if (request.inactivity() != null) {
              serviceStream = serviceStream.timeout(Duration.ofMillis(request.inactivity()));
            }

            Disposable disposable =
                serviceStream
                    .map(response -> prepareResponse(streamId, response, receivedErrorMessage))
                    .concatWith(Flux.defer(() -> prepareCompletion(streamId, receivedErrorMessage)))
                    .onErrorResume(t -> Mono.just(toErrorMessage(t, streamId)))
                    .doFinally(signalType -> session.dispose(streamId))
                    .subscribe(
                        response -> {
                          try {
                            sink.next(toByteBuf(response));
                          } catch (Throwable t) {
                            LOGGER.error("Failed to encode response message: {}", response, t);
                          }
                        },
                        sink::error,
                        sink::complete);

            session.register(sid, disposable);
          } catch (Throwable e) {
            Optional.ofNullable(request)
                .map(GatewayMessage::data)
                .ifPresent(ReferenceCountUtil::safestRelease);
            handleError(sink, session, sid, e);
          }
        });
  }

  private Mono<GatewayMessage> prepareCompletion(
      Long streamId, AtomicBoolean receivedErrorMessage) {
    return receivedErrorMessage.get()
        ? Mono.empty()
        : Mono.just(GatewayMessage.builder().streamId(streamId).signal(Signal.COMPLETE).build());
  }

  private GatewayMessage prepareResponse(
      Long streamId, ServiceMessage message, AtomicBoolean receivedErrorMessage) {
    GatewayMessage.Builder response = GatewayMessage.from(message).streamId(streamId);
    if (ExceptionProcessor.isError(message)) {
      receivedErrorMessage.set(true);
      response.signal(Signal.ERROR);
    }
    return response.build();
  }

  private void checkQualifierNotNull(WebsocketSession session, GatewayMessage request) {
    if (request.qualifier() == null) {
      LOGGER.error("Bad gateway request {} on session {}: qualifier is missing", request, session);
      throw new BadRequestException("qualifier is missing");
    }
  }

  private void checkSidNotRegisteredYet(
      Long streamId, WebsocketSession session, GatewayMessage request) {
    if (session.containsSid(streamId)) {
      LOGGER.error(
          "Bad gateway request {} on session {}: sid={} is already registered", request, session);
      throw new BadRequestException("sid=" + streamId + " is already registered");
    }
  }

  private void handleCancelRequest(
      FluxSink<ByteBuf> sink, Long streamId, WebsocketSession session, GatewayMessage request) {

    if (!session.dispose(streamId)) {
      LOGGER.error("CANCEL gateway request {} failed in session {}", request, streamId, session);
      throw new BadRequestException("Failed CANCEL request");
    }

    // release data, if for any reason client sent data inside CANCEL request
    Optional.ofNullable(request.data()).ifPresent(ReferenceCountUtil::safestRelease);

    // send ack message, if encoding throws here we're still safe
    sink.next(toByteBuf(cancelResponse(streamId)));
    sink.complete();
  }

  private void checkSidNotNull(Long streamId, WebsocketSession session, GatewayMessage request) {
    if (streamId == null) {
      LOGGER.error("Bad gateway request {} on session {}: sid is missing", request, session);
      throw new BadRequestException("sid is missing");
    }
  }

  private ByteBuf toByteBuf(GatewayMessage message) {
    return messageCodec.encode(message);
  }

  private GatewayMessage toMessage(ByteBuf message) {
    return messageCodec.decode(message);
  }

  private GatewayMessage toErrorMessage(Throwable t, Long streamId) {
    return GatewayMessage.from(ExceptionProcessor.toMessage(t))
        .streamId(streamId)
        .signal(Signal.ERROR)
        .build();
  }

  private GatewayMessage cancelResponse(Long streamId) {
    return GatewayMessage.builder().streamId(streamId).signal(Signal.CANCEL).build();
  }

  private void handleError(
      FluxSink<ByteBuf> sink, WebsocketSession session, Long sid, Throwable e) {
    try {
      sink.next(toByteBuf(toErrorMessage(e, sid)));
      sink.complete();
    } catch (Throwable t) {
      LOGGER.error(
          "Failed to send error message on session {}: on sid={}, muted cause={}",
          sid,
          session,
          e,
          t);
    }
  }

  private GatewayMessage enrichFromClient(GatewayMessage message) {
    return GatewayMessage.from(message)
        .header("gw-recv-from-client-time", System.currentTimeMillis())
        .build();
  }

  private ServiceMessage enrichFromService(ServiceMessage message) {
    return ServiceMessage.from(message)
        .header("gw-recv-from-service-time", String.valueOf(System.currentTimeMillis()))
        .build();
  }
}
