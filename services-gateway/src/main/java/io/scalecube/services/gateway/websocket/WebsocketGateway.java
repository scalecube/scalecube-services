package io.scalecube.services.gateway.websocket;

import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.scalecube.services.Address;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewaySessionHandler;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.StringJoiner;
import java.util.function.UnaryOperator;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class WebsocketGateway implements Gateway {

  private final GatewayOptions options;
  private final GatewaySessionHandler gatewayHandler;
  private final Duration keepAliveInterval;
  private final ServiceProviderErrorMapper errorMapper;

  private DisposableServer server;
  private LoopResources loopResources;

  private WebsocketGateway(Builder builder) {
    this.options = builder.options;
    this.gatewayHandler = builder.gatewayHandler;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.errorMapper = builder.errorMapper;
  }

  public WebsocketGateway(UnaryOperator<Builder> operator) {
    this(operator.apply(new Builder()));
  }

  @Override
  public String id() {
    return options.id();
  }

  @Override
  public Gateway start() {
    WebsocketGatewayAcceptor gatewayAcceptor =
        new WebsocketGatewayAcceptor(options.call(), gatewayHandler, errorMapper);

    loopResources =
        LoopResources.create(
            options.id() + ":" + options.port(), LoopResources.DEFAULT_IO_WORKER_COUNT, true);

    try {
      prepareHttpServer(loopResources, options.port())
          .doOnConnection(this::setupKeepAlive)
          .handle(gatewayAcceptor)
          .bind()
          .doOnSuccess(server -> this.server = server)
          .thenReturn(this)
          .toFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return this;
  }

  private HttpServer prepareHttpServer(LoopResources loopResources, int port) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              if (loopResources != null) {
                tcpServer = tcpServer.runOn(loopResources);
              }
              return tcpServer.bindAddress(() -> new InetSocketAddress(port));
            });
  }

  @Override
  public Address address() {
    InetSocketAddress address = (InetSocketAddress) server.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public void stop() {
    shutdownServer(server);
    shutdownLoopResources(loopResources);
  }

  private void shutdownServer(DisposableServer server) {
    if (server != null) {
      server.dispose();
    }
  }

  private void shutdownLoopResources(LoopResources loopResources) {
    if (loopResources != null) {
      loopResources.dispose();
    }
  }

  private void setupKeepAlive(Connection connection) {
    if (keepAliveInterval != Duration.ZERO) {
      connection
          .onReadIdle(keepAliveInterval.toMillis(), () -> onReadIdle(connection))
          .onWriteIdle(keepAliveInterval.toMillis(), () -> onWriteIdle(connection));
    }
  }

  private void onWriteIdle(Connection connection) {
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

  private void onReadIdle(Connection connection) {
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

  @Override
  public String toString() {
    return new StringJoiner(", ", WebsocketGateway.class.getSimpleName() + "[", "]")
        .add("options=" + options)
        .add("gatewayHandler=" + gatewayHandler)
        .add("keepAliveInterval=" + keepAliveInterval)
        .add("errorMapper=" + errorMapper)
        .add("server=" + server)
        .add("loopResources=" + loopResources)
        .toString();
  }

  public static class Builder {

    private GatewayOptions options;
    private GatewaySessionHandler gatewayHandler = GatewaySessionHandler.DEFAULT_INSTANCE;
    private Duration keepAliveInterval = Duration.ZERO;
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;

    public Builder() {}

    public GatewayOptions options() {
      return options;
    }

    public Builder options(GatewayOptions options) {
      this.options = options;
      return this;
    }

    public GatewaySessionHandler gatewayHandler() {
      return gatewayHandler;
    }

    public Builder gatewayHandler(GatewaySessionHandler gatewayHandler) {
      this.gatewayHandler = gatewayHandler;
      return this;
    }

    public Duration keepAliveInterval() {
      return keepAliveInterval;
    }

    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    public ServiceProviderErrorMapper errorMapper() {
      return errorMapper;
    }

    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public WebsocketGateway build() {
      return new WebsocketGateway(this);
    }
  }
}
