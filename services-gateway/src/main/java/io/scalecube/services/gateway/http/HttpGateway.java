package io.scalecube.services.gateway.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.services.Address;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import java.net.InetSocketAddress;
import java.util.StringJoiner;
import java.util.function.Consumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class HttpGateway implements Gateway {

  private final GatewayOptions options;
  private final ServiceProviderErrorMapper errorMapper;
  private final boolean corsEnabled;
  private final CorsConfigBuilder corsConfigBuilder;

  private DisposableServer server;
  private LoopResources loopResources;

  private HttpGateway(Builder builder) {
    this.options = builder.options;
    this.errorMapper = builder.errorMapper;
    this.corsEnabled = builder.corsEnabled;
    this.corsConfigBuilder = builder.corsConfigBuilder;
  }

  @Override
  public String id() {
    return options.id();
  }

  @Override
  public Mono<Gateway> start() {
    return Mono.defer(
        () -> {
          HttpGatewayAcceptor gatewayAcceptor =
              new HttpGatewayAcceptor(options.call(), errorMapper);

          loopResources = LoopResources.create(options.id() + ":" + options.port());

          return prepareHttpServer(loopResources, options.port())
              .handle(gatewayAcceptor)
              .bind()
              .doOnSuccess(server -> this.server = server)
              .thenReturn(this);
        });
  }

  private HttpServer prepareHttpServer(LoopResources loopResources, int port) {
    HttpServer httpServer = HttpServer.create();

    if (loopResources != null) {
      httpServer = httpServer.runOn(loopResources);
    }

    return httpServer
        .bindAddress(() -> new InetSocketAddress(port))
        .doOnConnection(
            connection -> {
              if (corsEnabled) {
                connection.addHandlerLast(new CorsHandler(corsConfigBuilder.build()));
              }
            });
  }

  @Override
  public Address address() {
    InetSocketAddress address = (InetSocketAddress) server.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public Mono<Void> stop() {
    return Flux.concatDelayError(shutdownServer(server), shutdownLoopResources(loopResources))
        .then();
  }

  private Mono<Void> shutdownServer(DisposableServer server) {
    return Mono.defer(
        () -> {
          if (server != null) {
            server.dispose();
            return server.onDispose();
          }
          return Mono.empty();
        });
  }

  private Mono<Void> shutdownLoopResources(LoopResources loopResources) {
    return loopResources != null ? loopResources.disposeLater() : Mono.empty();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", HttpGateway.class.getSimpleName() + "[", "]")
        .add("options=" + options)
        .add("errorMapper=" + errorMapper)
        .add("corsEnabled=" + corsEnabled)
        .add("corsConfigBuilder=" + corsConfigBuilder)
        .add("server=" + server)
        .add("loopResources=" + loopResources)
        .toString();
  }

  public static class Builder {

    private GatewayOptions options;
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private boolean corsEnabled = false;
    private CorsConfigBuilder corsConfigBuilder =
        CorsConfigBuilder.forAnyOrigin()
            .allowNullOrigin()
            .maxAge(3600)
            .allowedRequestMethods(HttpMethod.POST);

    public Builder() {}

    public GatewayOptions options() {
      return options;
    }

    public Builder options(GatewayOptions options) {
      this.options = options;
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
      consumer.accept(this.corsConfigBuilder);
      return this;
    }

    public HttpGateway build() {
      return new HttpGateway(this);
    }
  }
}
