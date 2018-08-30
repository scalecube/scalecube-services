package io.scalecube.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
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
        (WebsocketInbound inbound, WebsocketOutbound outbound) -> {
          WebsocketSession session = new WebsocketSession(httpRequest, inbound, outbound);
          Mono<Void> voidMono = onConnect(session);
          session.onClose(() -> onDisconnect(session));
          return voidMono;
        });
  }

  private Mono<Void> onConnect(WebsocketSession session) {
    LOGGER.info("Session connected: " + session);
    metrics.incrConnection();

    Mono<Void> voidMono =
        session.send(
            session
                .receive()
                .flatMap(
                    frame ->
                        Flux.<GatewayMessage>create(
                            sink -> {
                              Long sid = null;
                              try {
                                GatewayMessage request = toGatewayMessage(frame);
                                Long streamId = sid = request.streamId();

                                // check message contains sid
                                checkSidNotNull(streamId, session, request);
                                // check session contains sid for CANCEL operation
                                if (request.hasSignal(Signal.CANCEL)) {
                                  handleCancelRequest(streamId, session, request, sink);
                                  return;
                                }
                                // check session not yet contain sid
                                checkSessionHasSid(streamId, session, request);
                                // check message contains quailifier
                                checkQualifierNotNull(session, request);

                                metrics.markRequest();

                                AtomicBoolean receivedErrorMessage = new AtomicBoolean(false);

                                Flux<ServiceMessage> serviceStream =
                                    serviceCall.requestMany(
                                        GatewayMessage.toServiceMessage(request));

                                if (request.inactivity() != null) {
                                  serviceStream =
                                      serviceStream.timeout(
                                          Duration.ofMillis(request.inactivity()));
                                }

                                Disposable disposable =
                                    serviceStream
                                        .map(
                                            message -> {
                                              GatewayMessage.Builder response =
                                                  GatewayMessage.from(message).streamId(streamId);
                                              if (ExceptionProcessor.isError(message)) {
                                                receivedErrorMessage.set(true);
                                                response.signal(Signal.ERROR);
                                              }
                                              return response.build();
                                            })
                                        .concatWith(
                                            Flux.defer(
                                                () ->
                                                    receivedErrorMessage.get()
                                                        ? Mono.empty()
                                                        : Mono.just(
                                                            GatewayMessage.builder()
                                                                .streamId(streamId)
                                                                .signal(Signal.COMPLETE)
                                                                .build())))
                                        .onErrorResume(t -> Mono.just(toErrorMessage(t, streamId)))
                                        .doFinally(signalType -> session.dispose(streamId))
                                        .subscribe(sink::next, sink::error, sink::complete);
                                session.register(sid, disposable);
                              } catch (Throwable ex) {
                                ReferenceCountUtil.safeRelease(frame);
                                sink.next(toErrorMessage(ex, sid));
                                sink.complete();
                              }
                            }))
                .flatMap(this::toByteBuf)
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Unhandled exception occured: {}, " + "session: {} will be closed",
                            ex,
                            session,
                            ex)));

    session.onClose(
        () -> {
          LOGGER.info("Session disconnected: " + session);
          metrics.decrConnection();
        });

    return voidMono.then();
  }

  private void checkQualifierNotNull(WebsocketSession session, GatewayMessage request) {
    if (request.qualifier() == null) {
      LOGGER.error("Failed gateway request: {}, q is missing for session: {}", request, session);
      throw new BadRequestException("q is missing");
    }
  }

  private void checkSessionHasSid(Long streamId, WebsocketSession session, GatewayMessage request) {
    if (session.containsSid(streamId)) {
      LOGGER.error(
          "Failed gateway request: {}, " + "sid={} is already registered on session: {}",
          request,
          session);
      throw new BadRequestException("sid=" + streamId + " is already registered on session");
    }
  }

  private void handleCancelRequest(
      Long streamId,
      WebsocketSession session,
      GatewayMessage request,
      FluxSink<GatewayMessage> sink) {

    boolean dispose = session.dispose(streamId);

    if (!dispose) {
      LOGGER.error(
          "CANCEL failed for gateway request: {}, " + "sid={} is not contained in session: {}",
          request,
          streamId,
          session);
      throw new BadRequestException("sid=" + streamId + " is not contained in session");
    } else {
      sink.next(GatewayMessage.builder().streamId(streamId).signal(Signal.CANCEL).build());
      sink.complete();
    }
  }

  private void checkSidNotNull(Long streamId, WebsocketSession session, GatewayMessage request) {
    if (streamId == null) {
      LOGGER.error(
          "Invalid gateway request: {}, " + "sid is missing for session: {}", request, session);
      throw new BadRequestException("sid is missing");
    }
  }

  private Mono<Void> onDisconnect(WebsocketSession session) {
    LOGGER.info("Session disconnected: " + session);
    return Mono.empty();
  }

  private Mono<ByteBuf> toByteBuf(GatewayMessage message) {
    try {
      return Mono.just(messageCodec.encode(message));
    } catch (Throwable ex) {
      ReferenceCountUtil.safeRelease(message.data());
      return Mono.empty();
    }
  }

  private GatewayMessage toGatewayMessage(WebSocketFrame frame) {
    try {
      return messageCodec.decode(frame.content());
    } catch (Throwable ex) {
      // we will release it in catch block of the onConnect
      throw new BadRequestException(ex.getMessage());
    }
  }

  private GatewayMessage toErrorMessage(Throwable th, Long streamId) {
    ServiceMessage serviceMessage = ExceptionProcessor.toMessage(th);
    return GatewayMessage.from(serviceMessage).streamId(streamId).signal(Signal.ERROR).build();
  }
}
