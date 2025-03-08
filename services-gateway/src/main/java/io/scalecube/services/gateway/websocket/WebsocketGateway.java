package io.scalecube.services.gateway.websocket;

import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewaySessionHandler;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Function;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class WebsocketGateway implements Gateway {

  private final String id;
  private final int port;
  private final Function<ServiceCall, ServiceCall> callFactory;
  private final GatewaySessionHandler gatewayHandler;
  private final Duration keepAliveInterval;
  private final boolean heartbeatEnabled;
  private final ServiceProviderErrorMapper errorMapper;

  private DisposableServer server;
  private LoopResources loopResources;

  private WebsocketGateway(Builder builder) {
    this.id = builder.id;
    this.port = builder.port;
    this.callFactory = builder.callFactory;
    this.gatewayHandler = builder.gatewayHandler;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.heartbeatEnabled = builder.heartbeatEnabled;
    this.errorMapper = builder.errorMapper;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Gateway start(ServiceCall call, ServiceRegistry serviceRegistry) {
    loopResources =
        LoopResources.create(id + ":" + port, LoopResources.DEFAULT_IO_WORKER_COUNT, true);

    if (heartbeatEnabled) {
      serviceRegistry.registerService(
          ServiceInfo.fromServiceInstance(new HeartbeatServiceImpl())
              .errorMapper(DefaultErrorMapper.INSTANCE)
              .dataDecoder(ServiceMessageDataDecoder.INSTANCE)
              .build());
    }

    try {
      HttpServer.create()
          .runOn(loopResources)
          .bindAddress(() -> new InetSocketAddress(port))
          .doOnConnection(this::setupKeepAlive)
          .handle(
              new WebsocketGatewayAcceptor(callFactory.apply(call), gatewayHandler, errorMapper))
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

  public static class Builder {

    private String id = "websocket@" + Integer.toHexString(hashCode());
    private int port;
    private Function<ServiceCall, ServiceCall> callFactory = call -> call;
    private GatewaySessionHandler gatewayHandler = GatewaySessionHandler.DEFAULT_INSTANCE;
    private Duration keepAliveInterval = Duration.ZERO;
    private boolean heartbeatEnabled = false;
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;

    private Builder() {}

    public String id() {
      return id;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public int port() {
      return port;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder serviceCall(Function<ServiceCall, ServiceCall> operator) {
      callFactory = callFactory.andThen(operator);
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

    public boolean heartbeatEnabled() {
      return heartbeatEnabled;
    }

    public Builder heartbeatEnabled(boolean heartbeatEnabled) {
      this.heartbeatEnabled = heartbeatEnabled;
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
