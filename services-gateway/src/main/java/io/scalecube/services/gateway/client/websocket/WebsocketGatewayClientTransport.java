package io.scalecube.services.gateway.client.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

public final class WebsocketGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WebsocketGatewayClientTransport.class);

  private static final String STREAM_ID = "sid";
  private static final String CONTENT_TYPE = "application/json";
  private static final WebsocketGatewayClientCodec CLIENT_CODEC = new WebsocketGatewayClientCodec();
  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

  private final GatewayClientCodec clientCodec;
  private final Duration keepAliveInterval;
  private final Function<HttpClient, HttpClient> operator;

  private final AtomicLong sidCounter = new AtomicLong();
  private final Mono<WebsocketGatewayClientSession> clientSession;
  private final AtomicReference<WebsocketGatewayClientSession> clientSessionReference =
      new AtomicReference<>();
  private volatile boolean closed;

  private WebsocketGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.operator = builder.operator;
    this.clientSession =
        Mono.defer(this::connect)
            .cacheInvalidateIf(WebsocketGatewayClientSession::isDisposed);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    return this;
  }

  private Mono<WebsocketGatewayClientSession> connect() {
    if (closed) {
      return Mono.error(new IllegalStateException("Transport is closed"));
    }

    final HttpClient httpClient =
        operator.apply(
            HttpClient.create(ConnectionProvider.newConnection())
                .resolver(DefaultAddressResolverGroup.INSTANCE)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.TCP_NODELAY, true)
                .headers(headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE)));

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
              LOGGER.info("Created session: {}", session);
              // setup shutdown hook
              session
                  .onClose()
                  .doOnTerminate(() -> LOGGER.info("Closed session: {}", session))
                  .subscribe(
                      null,
                      th ->
                          LOGGER.warn(
                              "Exception on closing session: {}, cause: {}",
                              session,
                              th.toString()));
              return session;
            })
        .doOnSuccess(session -> {
          clientSessionReference.set(session);
          if (closed && clientSessionReference.compareAndSet(session, null)) {
            session.close().onErrorComplete().subscribe();
          }
        })
        .doOnError(ex -> LOGGER.warn("Failed to connect, cause: {}", ex.toString()))
        .onErrorMap(ex -> new RuntimeException(getRootCause(ex)));
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return clientSession.flatMap(
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
    return clientSession.flatMapMany(
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
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
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
    closed = true;
    final var session = clientSessionReference.getAndSet(null);
    if (session != null) {
      session.close().onErrorComplete().subscribe();
    }
  }

  public static class Builder {

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private Duration keepAliveInterval = Duration.ZERO;
    private Function<HttpClient, HttpClient> operator = client -> client;

    private Builder() {}

    public Builder clientCodec(GatewayClientCodec clientCodec) {
      this.clientCodec = clientCodec;
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
