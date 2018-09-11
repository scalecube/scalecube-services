package io.scalecube.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.time.Duration;
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
    metrics.incConnection();

    Mono<Void> voidMono =
        session.send(
            session
                .receive()
                .doOnNext(input -> metrics.markRequest())
                .flatMap(response -> handleResponse(session, response))
                .flatMap(this::toByteBuf)
                .doOnNext(input -> metrics.markResponse())
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Unhandled exception occurred: {}, " + "session: {} will be closed",
                            ex,
                            session,
                            ex)));

    session.onClose(
        () -> {
          LOGGER.info("Session disconnected: " + session);
          metrics.decConnection();
        });

    return voidMono.then();
  }

  private Flux<GatewayMessage> handleResponse(WebsocketSession session, ByteBuf response) {
    return Flux.create(
        sink -> {
          Long sid = null;
          try {
            GatewayMessage request = toGatewayMessage(response);
            Long streamId = sid = request.streamId();

            // check message contains sid
            checkSidNotNull(streamId, session, request);

            // check session contains sid for CANCEL operation
            if (request.hasSignal(Signal.CANCEL)) {
              handleCancelRequest(streamId, session, request, sink);
              return;
            }

            // check session doesn't contain sid yet
            checkSidNotRegisteredYet(streamId, session, request);

            // check message contains qualifier
            checkQualifierNotNull(session, request);

            AtomicBoolean receivedErrorMessage = new AtomicBoolean(false);

            Flux<ServiceMessage> serviceStream =
                serviceCall.requestMany(GatewayMessage.toServiceMessage(request));

            if (request.inactivity() != null) {
              serviceStream = serviceStream.timeout(Duration.ofMillis(request.inactivity()));
            }

            Disposable disposable =
                serviceStream
                    .map(message -> prepareResponse(streamId, message, receivedErrorMessage))
                    .concatWith(Flux.defer(() -> prepareCompletion(streamId, receivedErrorMessage)))
                    .onErrorResume(t -> Mono.just(toErrorMessage(t, streamId)))
                    .doFinally(signalType -> session.dispose(streamId))
                    .subscribe(sink::next, sink::error, sink::complete);

            session.register(sid, disposable);
          } catch (Throwable ex) {
            ReferenceCountUtil.safeRelease(response);
            sink.next(toErrorMessage(ex, sid));
            sink.complete();
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
      Long streamId,
      WebsocketSession session,
      GatewayMessage request,
      FluxSink<GatewayMessage> sink) {

    if (!session.dispose(streamId)) {
      LOGGER.error("CANCEL gateway request {} failed in session {}", request, streamId, session);
      throw new BadRequestException("Failed CANCEL request");
    }

    // send message with CANCEL signal
    sink.next(cancelResponse(streamId));
    sink.complete();
  }

  private void checkSidNotNull(Long streamId, WebsocketSession session, GatewayMessage request) {
    if (streamId == null) {
      LOGGER.error("Bad gateway request {} on session {}: sid is missing", request, session);
      throw new BadRequestException("sid is missing");
    }
  }

  private Mono<ByteBuf> toByteBuf(GatewayMessage message) {
    try {
      return Mono.just(messageCodec.encode(message));
    } catch (Throwable ex) {
      ReferenceCountUtil.safeRelease(message.data());
      return Mono.empty();
    }
  }

  private GatewayMessage toGatewayMessage(ByteBuf response) {
    try {
      return messageCodec.decode(response);
    } catch (Throwable ex) {
      // we will release it in catch block of the onConnect
      throw new BadRequestException(ex.getMessage());
    }
  }

  private GatewayMessage toErrorMessage(Throwable th, Long streamId) {
    ServiceMessage serviceMessage = ExceptionProcessor.toMessage(th);
    return GatewayMessage.from(serviceMessage).streamId(streamId).signal(Signal.ERROR).build();
  }

  private GatewayMessage cancelResponse(Long streamId) {
    return GatewayMessage.builder().streamId(streamId).signal(Signal.CANCEL).build();
  }
}
