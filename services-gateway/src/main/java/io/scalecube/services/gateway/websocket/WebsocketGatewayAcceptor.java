package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.websocket.GatewayMessages.RATE_LIMIT_FIELD;
import static io.scalecube.services.gateway.websocket.GatewayMessages.getSid;
import static io.scalecube.services.gateway.websocket.GatewayMessages.getSignal;
import static io.scalecube.services.gateway.websocket.GatewayMessages.newCancelMessage;
import static io.scalecube.services.gateway.websocket.GatewayMessages.newCompleteMessage;
import static io.scalecube.services.gateway.websocket.GatewayMessages.newResponseMessage;
import static io.scalecube.services.gateway.websocket.GatewayMessages.toErrorResponse;
import static io.scalecube.services.gateway.websocket.GatewayMessages.validateSidOnSession;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.GatewaySessionHandler;
import io.scalecube.services.gateway.ReferenceCountUtil;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableChannel;
import reactor.netty.channel.AbortedException;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;
import reactor.util.context.Context;

public class WebsocketGatewayAcceptor
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

  private static final int DEFAULT_ERROR_CODE = 500;

  private static final AtomicLong SESSION_ID_GENERATOR = new AtomicLong(System.currentTimeMillis());

  private final WebsocketServiceMessageCodec messageCodec = new WebsocketServiceMessageCodec();
  private final ServiceCall serviceCall;
  private final GatewaySessionHandler gatewayHandler;
  private final ServiceProviderErrorMapper errorMapper;

  /**
   * Constructor for websocket acceptor.
   *
   * @param serviceCall service call
   * @param gatewayHandler gateway handler
   * @param errorMapper error mapper
   */
  public WebsocketGatewayAcceptor(
      ServiceCall serviceCall,
      GatewaySessionHandler gatewayHandler,
      ServiceProviderErrorMapper errorMapper) {
    this.serviceCall = Objects.requireNonNull(serviceCall, "serviceCall");
    this.gatewayHandler = Objects.requireNonNull(gatewayHandler, "gatewayHandler");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
  }

  @Override
  public Publisher<Void> apply(HttpServerRequest httpRequest, HttpServerResponse httpResponse) {
    final Map<String, String> headers = computeHeaders(httpRequest.requestHeaders());
    final long sessionId = SESSION_ID_GENERATOR.incrementAndGet();

    return gatewayHandler
        .onConnectionOpen(sessionId, headers)
        .doOnError(
            ex ->
                httpResponse
                    .status(toStatusCode(ex))
                    .send()
                    .doFinally(s -> httpResponse.withConnection(DisposableChannel::dispose))
                    .subscribe())
        .then(
            Mono.defer(
                () ->
                    httpResponse.sendWebsocket(
                        (WebsocketInbound inbound, WebsocketOutbound outbound) ->
                            onConnect(
                                new WebsocketGatewaySession(
                                    sessionId,
                                    messageCodec,
                                    headers,
                                    inbound,
                                    outbound,
                                    gatewayHandler)))))
        .onErrorResume(throwable -> Mono.empty());
  }

  private static Map<String, String> computeHeaders(HttpHeaders httpHeaders) {
    // exception will be thrown on duplicate
    return httpHeaders.entries().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  private static int toStatusCode(Throwable throwable) {
    int status = DEFAULT_ERROR_CODE;
    if (throwable instanceof ServiceException) {
      if (throwable instanceof BadRequestException) {
        status = BadRequestException.ERROR_TYPE;
      } else if (throwable instanceof UnauthorizedException) {
        status = UnauthorizedException.ERROR_TYPE;
      } else if (throwable instanceof ForbiddenException) {
        status = ForbiddenException.ERROR_TYPE;
      } else if (throwable instanceof ServiceUnavailableException) {
        status = ServiceUnavailableException.ERROR_TYPE;
      } else if (throwable instanceof InternalServiceException) {
        status = InternalServiceException.ERROR_TYPE;
      }
    }
    return status;
  }

  private Mono<Void> onConnect(WebsocketGatewaySession session) {
    gatewayHandler.onSessionOpen(session);

    session
        .receive()
        .subscribe(
            byteBuf -> {
              if (byteBuf == Unpooled.EMPTY_BUFFER) {
                return;
              }

              if (!byteBuf.isReadable()) {
                ReferenceCountUtil.safestRelease(byteBuf);
                return;
              }

              Mono.deferContextual(context -> onRequest(session, byteBuf, (Context) context))
                  .contextWrite(context -> gatewayHandler.onRequest(session, byteBuf, context))
                  .subscribe();
            },
            th -> {
              if (!(th instanceof AbortedException)) {
                gatewayHandler.onSessionError(session, th);
              }
            });

    return session.onClose(() -> gatewayHandler.onSessionClose(session));
  }

  private Mono<ServiceMessage> onRequest(
      WebsocketGatewaySession session, ByteBuf byteBuf, Context context) {

    return Mono.fromCallable(() -> messageCodec.decode(byteBuf))
        .map(GatewayMessages::validateSid)
        .flatMap(message -> onCancel(session, message))
        .map(message -> validateSidOnSession(session, (ServiceMessage) message))
        .map(GatewayMessages::validateQualifier)
        .map(message -> gatewayHandler.mapMessage(session, message, context))
        .doOnNext(request -> onRequest(session, request, context))
        .doOnError(
            th -> {
              if (!(th instanceof WebsocketContextException)) {
                // decode failed at this point
                gatewayHandler.onError(session, th, context);
                return;
              }

              WebsocketContextException wex = (WebsocketContextException) th;
              wex.releaseRequest(); // release

              session
                  .send(toErrorResponse(errorMapper, wex.request(), wex.getCause()))
                  .contextWrite(context)
                  .subscribe();
            });
  }

  private void onRequest(WebsocketGatewaySession session, ServiceMessage request, Context context) {
    final long sid = getSid(request);
    final AtomicBoolean receivedError = new AtomicBoolean(false);

    Flux<ServiceMessage> serviceStream = serviceCall.requestMany(request);
    final String limitRate = request.header(RATE_LIMIT_FIELD);
    serviceStream =
        limitRate != null ? serviceStream.limitRate(Integer.parseInt(limitRate)) : serviceStream;

    Disposable disposable =
        session
            .send(
                serviceStream.map(
                    response -> {
                      boolean isErrorResponse = response.isError();
                      if (isErrorResponse) {
                        receivedError.set(true);
                      }
                      return newResponseMessage(sid, response, isErrorResponse);
                    }))
            .doOnError(
                th -> {
                  ReferenceCountUtil.safestRelease(request.data());
                  receivedError.set(true);
                  session
                      .send(toErrorResponse(errorMapper, request, th))
                      .contextWrite(context)
                      .subscribe();
                })
            .doOnTerminate(
                () -> {
                  if (!receivedError.get()) {
                    session
                        .send(newCompleteMessage(sid, request.qualifier()))
                        .contextWrite(context)
                        .subscribe();
                  }
                })
            .doFinally(signalType -> session.dispose(sid))
            .contextWrite(context)
            .subscribe();

    session.register(sid, disposable);
  }

  private Mono<?> onCancel(WebsocketGatewaySession session, ServiceMessage message) {
    if (getSignal(message) != Signal.CANCEL) {
      return Mono.just(message);
    }

    // release data if CANCEL contains data (it shouldn't normally)
    if (message.data() != null) {
      ReferenceCountUtil.safestRelease(message.data());
    }

    // dispose by sid (if anything to dispose)
    long sid = getSid(message);
    session.dispose(sid);

    // no need to subscribe here since flatMap will do
    return session.send(newCancelMessage(sid, message.qualifier()));
  }
}
