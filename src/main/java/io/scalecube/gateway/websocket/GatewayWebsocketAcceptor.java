package io.scalecube.gateway.websocket;

import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
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
            onConnect(new WebsocketSession(messageCodec, httpRequest, inbound, outbound)));
  }

  private Mono<Void> onConnect(WebsocketSession session) {
    LOGGER.info("Session connected: " + session);

    session
        .receive()
        .subscribe(
            byteBuf ->
                Mono.fromCallable(() -> messageCodec.decode(byteBuf))
                    .doOnNext(message -> metrics.markRequest())
                    .flatMap(this::checkSid)
                    .flatMap(msg -> handleCancelIfNeeded(session, msg))
                    .flatMap(msg -> checkSidNonce(session, (GatewayMessage) msg))
                    .flatMap(this::checkQualifier)
                    .subscribe(
                        request -> handleMessage(session, request),
                        th -> {
                          if (th instanceof WebsocketException) {
                            WebsocketException ex = (WebsocketException) th;
                            session
                                .send(ex.getCause(), ex.releaseRequest().request().streamId())
                                .subscribe();
                          } else {
                            // just log
                            LOGGER.error(
                                "Unhandled exception occurred: {}, session: {}", th, session.id());
                          }
                        }));

    return session.onClose(() -> LOGGER.info("Session disconnected: " + session));
  }

  private void handleMessage(WebsocketSession session, GatewayMessage request) {
    Long sid = request.streamId();

    AtomicBoolean receivedErrorMessage = new AtomicBoolean(false);

    Flux<ServiceMessage> serviceStream =
        serviceCall.requestMany(GatewayMessage.toServiceMessage(request));

    Disposable disposable =
        serviceStream
            .map(response -> prepareResponse(sid, response, receivedErrorMessage))
            .concatWith(Mono.defer(() -> prepareCompletion(sid, receivedErrorMessage)))
            .onErrorResume(t -> Mono.just(toErrorMessage(t, sid)))
            .doFinally(signalType -> session.dispose(sid))
            .subscribe(
                response ->
                    session
                        .send(response)
                        .doOnSuccess(avoid -> markResponse(response))
                        .subscribe());

    session.register(sid, disposable);
  }

  private void markResponse(GatewayMessage response) {
    if (!response.hasHeader("sig")) {
      metrics.markResponse();
    }
  }

  private Mono<GatewayMessage> checkQualifier(GatewayMessage msg) {
    if (msg.qualifier() == null) {
      throw WebsocketException.newBadRequest("qualifier is missing", msg);
    }
    return Mono.just(msg);
  }

  private Mono<GatewayMessage> checkSidNonce(WebsocketSession session, GatewayMessage msg) {
    if (session.containsSid(msg.streamId())) {
      throw WebsocketException.newBadRequest(
          "sid=" + msg.streamId() + " is already registered", msg);
    } else {
      return Mono.just(msg);
    }
  }

  private Mono<?> handleCancelIfNeeded(WebsocketSession session, GatewayMessage msg) {
    if (!msg.hasSignal(Signal.CANCEL)) {
      return Mono.just(msg);
    }

    if (!session.dispose(msg.streamId())) {
      throw WebsocketException.newBadRequest("Failed CANCEL request", msg);
    }

    GatewayMessage cancelAck =
        GatewayMessage.builder().streamId(msg.streamId()).signal(Signal.CANCEL).build();
    return session.send(cancelAck).then();
  }

  private Mono<GatewayMessage> checkSid(GatewayMessage msg) {
    if (msg.streamId() == null) {
      throw WebsocketException.newBadRequest("sid is missing", msg);
    } else {
      return Mono.just(msg);
    }
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

  private GatewayMessage toErrorMessage(Throwable t, Long streamId) {
    return GatewayMessage.from(ExceptionProcessor.toMessage(t))
        .streamId(streamId)
        .signal(Signal.ERROR)
        .build();
  }
}
