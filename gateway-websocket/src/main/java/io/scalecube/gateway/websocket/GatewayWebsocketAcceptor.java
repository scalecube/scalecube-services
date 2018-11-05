package io.scalecube.gateway.websocket;

import io.scalecube.gateway.GatewayMetrics;
import io.scalecube.gateway.ReferenceCountUtil;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessage.Builder;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

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
                    .map(this::checkSid)
                    .flatMap(msg -> handleCancel(session, msg))
                    .map(msg -> checkSidNonce(session, (GatewayMessage) msg))
                    .map(this::checkQualifier)
                    .subscribe(
                        request -> handleMessage(session, request),
                        th -> {
                          LOGGER.error("Exception occurred: {}, session={}", th, session.id());
                          if (th instanceof WebsocketRequestException) {
                            WebsocketRequestException ex = (WebsocketRequestException) th;
                            session
                                .send(ex.getCause(), ex.releaseRequest().request().streamId())
                                .subscribe();
                          }
                        }));

    return session.onClose(() -> LOGGER.info("Session disconnected: " + session));
  }

  private void handleMessage(WebsocketSession session, GatewayMessage request) {
    Long sid = request.streamId();

    AtomicBoolean receivedError = new AtomicBoolean(false);

    final Flux<ServiceMessage> serviceStream =
        serviceCall.requestMany(GatewayMessage.toServiceMessage(request));

    Disposable disposable =
        Optional.ofNullable(request.rateLimit())
            .map(serviceStream::limitRate)
            .orElse(serviceStream)
            .map(response -> prepareResponse(sid, response, receivedError))
            .doOnNext(response -> metrics.markServiceResponse())
            .doFinally(signalType -> session.dispose(sid))
            .subscribe(
                response ->
                    session.send(response).doOnSuccess(avoid -> metrics.markResponse()).subscribe(),
                th -> {
                  LOGGER.error(
                      "Exception occurred on request: {}, session={}, cause: {}",
                      request,
                      session.id(),
                      th);
                  handleError(session, sid, th);
                },
                () -> {
                  // handle complete
                  handleCompletion(session, sid, receivedError);
                });

    session.register(sid, disposable);
  }

  private void handleError(WebsocketSession session, Long sid, Throwable th) {
    Builder builder = GatewayMessage.from(ExceptionProcessor.toMessage(th));
    Optional.ofNullable(sid).ifPresent(builder::streamId);
    GatewayMessage response = builder.signal(Signal.ERROR).build();
    session.send(response).subscribe();
  }

  private void handleCompletion(WebsocketSession session, Long sid, AtomicBoolean receivedError) {
    if (!receivedError.get()) {
      Builder builder = GatewayMessage.builder();
      Optional.ofNullable(sid).ifPresent(builder::streamId);
      GatewayMessage response = builder.signal(Signal.COMPLETE).build();
      session.send(response).subscribe();
    }
  }

  private GatewayMessage checkQualifier(GatewayMessage msg) {
    if (msg.qualifier() == null) {
      throw WebsocketRequestException.newBadRequest("qualifier is missing", msg);
    }
    return msg;
  }

  private GatewayMessage checkSidNonce(WebsocketSession session, GatewayMessage msg) {
    if (session.containsSid(msg.streamId())) {
      throw WebsocketRequestException.newBadRequest(
          "sid=" + msg.streamId() + " is already registered", msg);
    } else {
      return msg;
    }
  }

  private Mono<?> handleCancel(WebsocketSession session, GatewayMessage msg) {
    if (!msg.hasSignal(Signal.CANCEL)) {
      return Mono.just(msg);
    }

    if (!session.dispose(msg.streamId())) {
      throw WebsocketRequestException.newBadRequest("Failed CANCEL request", msg);
    }
    // release data if CANCEL contains data (it shouldn't normally), just in case
    Optional.ofNullable(msg.data()).ifPresent(ReferenceCountUtil::safestRelease);

    GatewayMessage cancelAck =
        GatewayMessage.builder().streamId(msg.streamId()).signal(Signal.CANCEL).build();
    return session.send(cancelAck); // no need to subscribe here since flatMap will do
  }

  private GatewayMessage checkSid(GatewayMessage msg) {
    if (msg.streamId() == null) {
      throw WebsocketRequestException.newBadRequest("sid is missing", msg);
    } else {
      return msg;
    }
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
}
