package io.scalecube.services.gateway.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import java.net.InetSocketAddress;
import java.util.StringJoiner;
import java.util.function.Consumer;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class HttpGateway implements Gateway {

  private final String id;
  private final int port;
  private final ServiceCall serviceCall;
  private final ServiceProviderErrorMapper errorMapper;
  private final boolean corsEnabled;
  private final CorsConfigBuilder corsConfigBuilder;

  private DisposableServer server;
  private LoopResources loopResources;

  private HttpGateway(Builder builder) {
    this.id = builder.id;
    this.port = builder.port;
    this.serviceCall = builder.serviceCall;
    this.errorMapper = builder.errorMapper;
    this.corsEnabled = builder.corsEnabled;
    this.corsConfigBuilder = builder.corsConfigBuilder;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Gateway start() {
    HttpGatewayAcceptor gatewayAcceptor = new HttpGatewayAcceptor(serviceCall, errorMapper);

    loopResources =
        LoopResources.create(id + ":" + port, LoopResources.DEFAULT_IO_WORKER_COUNT, true);

    try {
      HttpServer.create()
          .runOn(loopResources)
          .bindAddress(() -> new InetSocketAddress(port))
          .doOnConnection(
              connection -> {
                if (corsEnabled) {
                  connection.addHandlerLast(new CorsHandler(corsConfigBuilder.build()));
                }
              })
          .handle(gatewayAcceptor)
          .bind()
          .doOnSuccess(server -> this.server = server)
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

  @Override
  public String toString() {
    return new StringJoiner(", ", HttpGateway.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("port=" + port)
        .add("serviceCall=" + serviceCall)
        .add("errorMapper=" + errorMapper)
        .add("corsEnabled=" + corsEnabled)
        .add("corsConfigBuilder=" + corsConfigBuilder)
        .add("server=" + server)
        .add("loopResources=" + loopResources)
        .toString();
  }

  public static class Builder {

    private String id = "http@" + Integer.toHexString(hashCode());
    private int port;
    private ServiceCall serviceCall;
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private boolean corsEnabled = false;
    private CorsConfigBuilder corsConfigBuilder =
        CorsConfigBuilder.forAnyOrigin()
            .allowNullOrigin()
            .maxAge(3600)
            .allowedRequestMethods(HttpMethod.POST);

    public Builder() {}

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

    public ServiceCall serviceCall() {
      return serviceCall;
    }

    public Builder serviceCall(ServiceCall serviceCall) {
      this.serviceCall = serviceCall;
      return this;
    }

    public ServiceProviderErrorMapper errorMapper() {
      return errorMapper;
    }

    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public boolean corsEnabled() {
      return corsEnabled;
    }

    public Builder corsEnabled(boolean corsEnabled) {
      this.corsEnabled = corsEnabled;
      return this;
    }

    public CorsConfigBuilder corsConfigBuilder() {
      return corsConfigBuilder;
    }

    public Builder corsConfigBuilder(CorsConfigBuilder corsConfigBuilder) {
      this.corsConfigBuilder = corsConfigBuilder;
      return this;
    }

    public Builder corsConfigBuilder(Consumer<CorsConfigBuilder> consumer) {
      consumer.accept(corsConfigBuilder);
      return this;
    }

    public HttpGateway build() {
      return new HttpGateway(this);
    }
  }
}
