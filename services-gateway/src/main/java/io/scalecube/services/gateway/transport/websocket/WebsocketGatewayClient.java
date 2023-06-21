package io.scalecube.services.gateway.transport.websocket;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.transport.GatewayClient;
import io.scalecube.services.gateway.transport.GatewayClientCodec;
import io.scalecube.services.gateway.transport.GatewayClientSettings;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class WebsocketGatewayClient implements GatewayClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketGatewayClient.class);

  private static final String STREAM_ID = "sid";

  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<WebsocketGatewayClient, Mono>
      websocketMonoUpdater =
          AtomicReferenceFieldUpdater.newUpdater(
              WebsocketGatewayClient.class, Mono.class, "websocketMono");

  private final AtomicLong sidCounter = new AtomicLong();

  private final GatewayClientCodec<ByteBuf> codec;
  private final GatewayClientSettings settings;
  private final HttpClient httpClient;
  private final LoopResources loopResources;
  private final boolean ownsLoopResources;

  private final Sinks.One<Void> close = Sinks.one();
  private final Sinks.One<Void> onClose = Sinks.one();

  @SuppressWarnings("unused")
  private volatile Mono<?> websocketMono;

  /**
   * Creates instance of websocket client transport.
   *
   * @param settings client settings
   * @param codec client codec.
   */
  public WebsocketGatewayClient(GatewayClientSettings settings, GatewayClientCodec<ByteBuf> codec) {
    this(settings, codec, LoopResources.create("websocket-gateway-client"), true);
  }

  /**
   * Creates instance of websocket client transport.
   *
   * @param settings client settings
   * @param codec client codec.
   * @param loopResources loopResources.
   */
  public WebsocketGatewayClient(
      GatewayClientSettings settings,
      GatewayClientCodec<ByteBuf> codec,
      LoopResources loopResources) {
    this(settings, codec, loopResources, false);
  }

  private WebsocketGatewayClient(
      GatewayClientSettings settings,
      GatewayClientCodec<ByteBuf> codec,
      LoopResources loopResources,
      boolean ownsLoopResources) {

    this.settings = settings;
    this.codec = codec;
    this.loopResources = loopResources;
    this.ownsLoopResources = ownsLoopResources;

    HttpClient httpClient =
        HttpClient.create(ConnectionProvider.newConnection())
            .headers(headers -> settings.headers().forEach(headers::add))
            .followRedirect(settings.followRedirect())
            .wiretap(settings.wiretap())
            .runOn(loopResources)
            .host(settings.host())
            .port(settings.port());

    if (settings.sslProvider() != null) {
      httpClient = httpClient.secure(settings.sslProvider());
    }

    this.httpClient = httpClient;

    // Setup cleanup
    close
        .asMono()
        .then(doClose())
        .doFinally(s -> onClose.emitEmpty(RETRY_NON_SERIALIZED))
        .doOnTerminate(() -> LOGGER.info("Closed client"))
        .subscribe(null, ex -> LOGGER.warn("Failed to close client, cause: " + ex));
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return getOrConnect()
        .flatMap(
            session -> {
              long sid = sidCounter.incrementAndGet();
              return session
                  .send(encodeRequest(request, sid))
                  .doOnSubscribe(s -> LOGGER.debug("Sending request {}", request))
                  .then(session.<ServiceMessage>newMonoProcessor(sid).asMono())
                  .doOnCancel(() -> session.cancel(sid, request.qualifier()))
                  .doFinally(s -> session.removeProcessor(sid));
            });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return getOrConnect()
        .flatMapMany(
            session -> {
              long sid = sidCounter.incrementAndGet();
              return session
                  .send(encodeRequest(request, sid))
                  .doOnSubscribe(s -> LOGGER.debug("Sending request {}", request))
                  .thenMany(session.<ServiceMessage>newUnicastProcessor(sid).asFlux())
                  .doOnCancel(() -> session.cancel(sid, request.qualifier()))
                  .doFinally(s -> session.removeProcessor(sid));
            });
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> requests) {
    return Flux.error(new UnsupportedOperationException("requestChannel is not supported"));
  }

  @Override
  public void close() {
    close.emitEmpty(RETRY_NON_SERIALIZED);
  }

  @Override
  public Mono<Void> onClose() {
    return onClose.asMono();
  }

  private Mono<Void> doClose() {
    return ownsLoopResources ? Mono.defer(loopResources::disposeLater) : Mono.empty();
  }

  private Mono<WebsocketGatewayClientSession> getOrConnect() {
    // noinspection unchecked
    return websocketMonoUpdater.updateAndGet(this, this::getOrConnect0);
  }

  private Mono<WebsocketGatewayClientSession> getOrConnect0(
      Mono<WebsocketGatewayClientSession> prev) {
    if (prev != null) {
      return prev;
    }

    Duration keepAliveInterval = settings.keepAliveInterval();

    return httpClient
        .websocket()
        .uri("/")
        .connect()
        .map(
            connection ->
                keepAliveInterval != Duration.ZERO
                    ? connection
                        .onReadIdle(keepAliveInterval.toMillis(), () -> onReadIdle(connection))
                        .onWriteIdle(keepAliveInterval.toMillis(), () -> onWriteIdle(connection))
                    : connection)
        .map(
            connection -> {
              WebsocketGatewayClientSession session =
                  new WebsocketGatewayClientSession(codec, connection);
              LOGGER.info("Created session: {}", session);
              // setup shutdown hook
              session
                  .onClose()
                  .doOnTerminate(
                      () -> {
                        websocketMonoUpdater.getAndSet(this, null); // clear reference
                        LOGGER.info("Closed session: {}", session);
                      })
                  .subscribe(
                      null,
                      th ->
                          LOGGER.warn(
                              "Exception on closing session: {}, cause: {}",
                              session,
                              th.toString()));
              return session;
            })
        .doOnError(
            ex -> {
              LOGGER.warn(
                  "Failed to connect on {}:{}, cause: {}", settings.host(), settings.port(), ex);
              websocketMonoUpdater.getAndSet(this, null); // clear reference
            })
        .cache();
  }

  private void onWriteIdle(Connection connection) {
    LOGGER.debug("Sending keepalive on writeIdle");
    connection
        .outbound()
        .sendObject(new PingWebSocketFrame())
        .then()
        .subscribe(null, ex -> LOGGER.warn("Can't send keepalive on writeIdle: " + ex));
  }

  private void onReadIdle(Connection connection) {
    LOGGER.debug("Sending keepalive on readIdle");
    connection
        .outbound()
        .sendObject(new PingWebSocketFrame())
        .then()
        .subscribe(null, ex -> LOGGER.warn("Can't send keepalive on readIdle: " + ex));
  }

  private ByteBuf encodeRequest(ServiceMessage message, long sid) {
    return codec.encode(ServiceMessage.from(message).header(STREAM_ID, sid).build());
  }
}
