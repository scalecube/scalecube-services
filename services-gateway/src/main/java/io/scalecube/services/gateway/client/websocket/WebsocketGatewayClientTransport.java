package io.scalecube.services.gateway.client.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.gateway.client.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class WebsocketGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER =
      System.getLogger(WebsocketGatewayClientTransport.class.getName());

  private static final String STREAM_ID = "sid";
  private static final String CONTENT_TYPE = "application/json";
  private static final WebsocketGatewayClientCodec CLIENT_CODEC = new WebsocketGatewayClientCodec();
  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

  private final GatewayClientCodec clientCodec;
  private final LoopResources loopResources;
  private final Duration keepAliveInterval;
  private final Function<HttpClient, HttpClient> operator;
  private final boolean ownsLoopResources;

  private final AtomicLong sidCounter = new AtomicLong();
  private final AtomicReference<WebsocketGatewayClientSession> clientSessionReference =
      new AtomicReference<>();

  private WebsocketGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.operator = builder.operator;
    this.loopResources =
        builder.loopResources == null
            ? LoopResources.create("websocket-gateway-client", 1, true)
            : builder.loopResources;
    this.ownsLoopResources = builder.loopResources == null;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    clientSessionReference.getAndUpdate(
        oldValue -> {
          if (oldValue != null) {
            return oldValue;
          }

          final HttpClient httpClient =
              operator.apply(
                  HttpClient.create(ConnectionProvider.newConnection())
                      .runOn(loopResources)
                      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                      .option(ChannelOption.TCP_NODELAY, true)
                      .headers(headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE)));

          return clientSession(httpClient);
        });
    return this;
  }

  private WebsocketGatewayClientSession clientSession(HttpClient httpClient) {
    try {
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
                final WebsocketGatewayClientSession session =
                    new WebsocketGatewayClientSession(clientCodec, connection);
                LOGGER.log(Level.INFO, "Created session: {0}", session);
                // setup shutdown hook
                session
                    .onClose()
                    .doOnTerminate(() -> LOGGER.log(Level.INFO, "Closed session: {0}", session))
                    .subscribe(
                        null,
                        th ->
                            LOGGER.log(
                                Level.WARNING,
                                "Exception on closing session: {0}, cause: {1}",
                                session,
                                th.toString()));
                return session;
              })
          .doOnError(
              ex -> LOGGER.log(Level.WARNING, "Failed to connect, cause: {0}", ex.toString()))
          .toFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(getRootCause(e));
    }
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request, Type responseType) {
    return Mono.defer(
        () -> {
          final var sid = sidCounter.incrementAndGet();
          final var session = clientSessionReference.get();
          return session
              .send(encodeRequest(request, sid))
              .doOnSubscribe(s -> LOGGER.log(Level.DEBUG, "Sending request {0}", request))
              .then(session.<ServiceMessage>newMonoProcessor(sid).asMono())
              .map(msg -> ServiceMessageCodec.decodeData(msg, responseType))
              .doOnCancel(() -> session.cancel(sid, request.qualifier()))
              .doFinally(s -> session.removeProcessor(sid));
        });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request, Type responseType) {
    return Flux.defer(
        () -> {
          final var sid = sidCounter.incrementAndGet();
          final var session = clientSessionReference.get();
          return session
              .send(encodeRequest(request, sid))
              .doOnSubscribe(s -> LOGGER.log(Level.DEBUG, "Sending request {0}", request))
              .thenMany(session.<ServiceMessage>newUnicastProcessor(sid).asFlux())
              .map(msg -> ServiceMessageCodec.decodeData(msg, responseType))
              .doOnCancel(() -> session.cancel(sid, request.qualifier()))
              .doFinally(s -> session.removeProcessor(sid));
        });
  }

  @Override
  public Flux<ServiceMessage> requestChannel(
      Publisher<ServiceMessage> publisher, Type responseType) {
    return Flux.error(new UnsupportedOperationException("requestChannel is not supported"));
  }

  private static void onWriteIdle(Connection connection) {
    connection
        .outbound()
        .sendObject(new PingWebSocketFrame())
        .then()
        .subscribe(
            null,
            ex -> {
              // no-op
            });
  }

  private static void onReadIdle(Connection connection) {
    connection
        .outbound()
        .sendObject(new PingWebSocketFrame())
        .then()
        .subscribe(
            null,
            ex -> {
              // no-op
            });
  }

  private ByteBuf encodeRequest(ServiceMessage message, long sid) {
    return clientCodec.encode(ServiceMessage.from(message).header(STREAM_ID, sid).build());
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause = throwable.getCause();
    return (cause == null) ? throwable : getRootCause(cause);
  }

  @Override
  public void close() {
    if (ownsLoopResources) {
      loopResources.dispose();
    }
  }

  public static class Builder {

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private LoopResources loopResources;
    private Duration keepAliveInterval = Duration.ZERO;
    private Function<HttpClient, HttpClient> operator = client -> client;

    public Builder() {}

    public Builder clientCodec(GatewayClientCodec clientCodec) {
      this.clientCodec = clientCodec;
      return this;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = loopResources;
      return this;
    }

    public Builder httpClient(UnaryOperator<HttpClient> operator) {
      this.operator = this.operator.andThen(operator);
      return this;
    }

    public Builder address(Address address) {
      return httpClient(client -> client.host(address.host()).port(address.port()));
    }

    public Builder connectTimeout(Duration connectTimeout) {
      return httpClient(
          client ->
              client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis()));
    }

    public Builder contentType(String contentType) {
      return httpClient(
          client ->
              client.headers(headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, contentType)));
    }

    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      return httpClient(client -> client.headers(entries -> headers.forEach(entries::set)));
    }

    public WebsocketGatewayClientTransport build() {
      return new WebsocketGatewayClientTransport(this);
    }
  }
}
