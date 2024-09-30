package io.scalecube.services.gateway.client.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;

public final class WebsocketGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WebsocketGatewayClientTransport.class);

  private static final String CONTENT_TYPE = "application/json";
  private static final String DEFAULT_HOST = "localhost";
  private static final String STREAM_ID = "sid";

  private static final LoopResources LOOP_RESOURCES =
      LoopResources.create("websocket-gateway-client");
  private static final WebsocketGatewayClientCodec CLIENT_CODEC = new WebsocketGatewayClientCodec();

  private final GatewayClientCodec clientCodec;
  private final LoopResources loopResources;
  private final String host;
  private final int port;
  private final Duration connectTimeout;
  private final String contentType;
  private final boolean followRedirect;
  private final SslProvider sslProvider;
  private final boolean shouldWiretap;
  private final Duration keepAliveInterval;
  private final Map<String, String> headers;

  private final AtomicLong sidCounter = new AtomicLong();
  private ConnectionProvider connectionProvider;
  private final AtomicReference<WebsocketGatewayClientSession> clientSessionReference =
      new AtomicReference<>();

  private WebsocketGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.loopResources = builder.loopResources;
    this.host = builder.host;
    this.port = builder.port;
    this.connectTimeout = builder.connectTimeout;
    this.contentType = builder.contentType;
    this.followRedirect = builder.followRedirect;
    this.sslProvider = builder.sslProvider;
    this.shouldWiretap = builder.shouldWiretap;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.headers = builder.headers;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    clientSessionReference.getAndUpdate(
        oldValue -> {
          if (oldValue != null) {
            return oldValue;
          }

          connectionProvider = ConnectionProvider.newConnection();

          HttpClient httpClient =
              HttpClient.create(connectionProvider)
                  .headers(entries -> headers.forEach(entries::add))
                  .headers(entries -> entries.set("Content-Type", contentType))
                  .followRedirect(followRedirect)
                  .wiretap(shouldWiretap)
                  .runOn(loopResources)
                  .host(host)
                  .port(port)
                  .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                  .option(ChannelOption.TCP_NODELAY, true);

          if (sslProvider != null) {
            httpClient = httpClient.secure(sslProvider);
          }

          return createClientSession(httpClient);
        });
    return this;
  }

  private WebsocketGatewayClientSession createClientSession(HttpClient httpClient) {
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
                WebsocketGatewayClientSession session =
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
          .doOnError(
              ex -> LOGGER.warn("Failed to connect on {}:{}, cause: {}", host, port, ex.toString()))
          .toFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request, Type responseType) {
    return Mono.defer(
        () -> {
          long sid = sidCounter.incrementAndGet();
          final WebsocketGatewayClientSession session = clientSessionReference.get();
          return session
              .send(encodeRequest(request, sid))
              .doOnSubscribe(s -> LOGGER.debug("Sending request {}", request))
              .then(session.<ServiceMessage>newMonoProcessor(sid).asMono())
              .doOnCancel(() -> session.cancel(sid, request.qualifier()))
              .doFinally(s -> session.removeProcessor(sid));
        });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request, Type responseType) {
    return Flux.defer(
        () -> {
          long sid = sidCounter.incrementAndGet();
          final WebsocketGatewayClientSession session = clientSessionReference.get();
          return session
              .send(encodeRequest(request, sid))
              .doOnSubscribe(s -> LOGGER.debug("Sending request {}", request))
              .thenMany(session.<ServiceMessage>newUnicastProcessor(sid).asFlux())
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

  @Override
  public void close() {
    if (connectionProvider != null) {
      connectionProvider.dispose();
    }
  }

  public static class Builder {

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private LoopResources loopResources = LOOP_RESOURCES;
    private String host = DEFAULT_HOST;
    private int port;
    private Duration connectTimeout = Duration.ofSeconds(5);
    private String contentType = CONTENT_TYPE;
    private boolean followRedirect;
    private SslProvider sslProvider;
    private boolean shouldWiretap;
    private Duration keepAliveInterval = Duration.ZERO;
    private Map<String, String> headers;

    public Builder() {}

    public GatewayClientCodec clientCodec() {
      return clientCodec;
    }

    public Builder clientCodec(GatewayClientCodec clientCodec) {
      this.clientCodec = clientCodec;
      return this;
    }

    public LoopResources loopResources() {
      return loopResources;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = loopResources;
      return this;
    }

    public String host() {
      return host;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public int port() {
      return port;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Duration connectTimeout() {
      return connectTimeout;
    }

    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public String contentType() {
      return contentType;
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public boolean isFollowRedirect() {
      return followRedirect;
    }

    public Builder followRedirect(boolean followRedirect) {
      this.followRedirect = followRedirect;
      return this;
    }

    public SslProvider sslProvider() {
      return sslProvider;
    }

    public Builder sslProvider(SslProvider sslProvider) {
      this.sslProvider = sslProvider;
      return this;
    }

    public boolean isShouldWiretap() {
      return shouldWiretap;
    }

    public Builder shouldWiretap(boolean shouldWiretap) {
      this.shouldWiretap = shouldWiretap;
      return this;
    }

    public Duration keepAliveInterval() {
      return keepAliveInterval;
    }

    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    public Map<String, String> headers() {
      return headers;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
      return this;
    }

    public WebsocketGatewayClientTransport builder() {
      return new WebsocketGatewayClientTransport(this);
    }
  }
}
