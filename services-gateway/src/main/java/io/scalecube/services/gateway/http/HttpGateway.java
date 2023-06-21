package io.scalecube.services.gateway.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.net.Address;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewayTemplate;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.function.UnaryOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class HttpGateway extends GatewayTemplate {

  private final ServiceProviderErrorMapper errorMapper;

  private DisposableServer server;
  private LoopResources loopResources;

  private boolean corsEnabled = false;
  private CorsConfigBuilder corsConfigBuilder =
      CorsConfigBuilder.forAnyOrigin()
          .allowNullOrigin()
          .maxAge(3600)
          .allowedRequestMethods(HttpMethod.POST);

  public HttpGateway(GatewayOptions options) {
    this(options, DefaultErrorMapper.INSTANCE);
  }

  public HttpGateway(GatewayOptions options, ServiceProviderErrorMapper errorMapper) {
    super(options);
    this.errorMapper = errorMapper;
  }

  private HttpGateway(HttpGateway other) {
    super(other.options);
    this.server = other.server;
    this.loopResources = other.loopResources;
    this.corsEnabled = other.corsEnabled;
    this.corsConfigBuilder = copy(other.corsConfigBuilder);
    this.errorMapper = other.errorMapper;
  }

  /**
   * CORS enable.
   *
   * @param corsEnabled if set to true.
   * @return HttpGateway with CORS settings.
   */
  public HttpGateway corsEnabled(boolean corsEnabled) {
    HttpGateway g = new HttpGateway(this);
    g.corsEnabled = corsEnabled;
    return g;
  }

  /**
   * Configure CORS with options.
   *
   * @param op for CORS.
   * @return HttpGateway with CORS settings.
   */
  public HttpGateway corsConfig(UnaryOperator<CorsConfigBuilder> op) {
    HttpGateway g = new HttpGateway(this);
    g.corsConfigBuilder = copy(op.apply(g.corsConfigBuilder));
    return g;
  }

  private CorsConfigBuilder copy(CorsConfigBuilder other) {
    CorsConfig config = other.build();
    CorsConfigBuilder corsConfigBuilder;
    if (config.isAnyOriginSupported()) {
      corsConfigBuilder = CorsConfigBuilder.forAnyOrigin();
    } else {
      corsConfigBuilder = CorsConfigBuilder.forOrigins(config.origins().toArray(new String[0]));
    }

    if (!config.isCorsSupportEnabled()) {
      corsConfigBuilder.disable();
    }

    corsConfigBuilder
        .exposeHeaders(config.exposedHeaders().toArray(new String[0]))
        .allowedRequestHeaders(config.allowedRequestHeaders().toArray(new String[0]))
        .allowedRequestMethods(config.allowedRequestMethods().toArray(new HttpMethod[0]))
        .maxAge(config.maxAge());

    for (Entry<String, String> header : config.preflightResponseHeaders()) {
      corsConfigBuilder.preflightResponseHeader(header.getKey(), header.getValue());
    }

    if (config.isShortCircuit()) {
      corsConfigBuilder.shortCircuit();
    }

    if (config.isNullOriginAllowed()) {
      corsConfigBuilder.allowNullOrigin();
    }

    if (config.isCredentialsAllowed()) {
      corsConfigBuilder.allowCredentials();
    }

    return corsConfigBuilder;
  }

  @Override
  public Mono<Gateway> start() {
    return Mono.defer(
        () -> {
          HttpGatewayAcceptor acceptor = new HttpGatewayAcceptor(options.call(), errorMapper);

          loopResources = LoopResources.create("http-gateway");

          return prepareHttpServer(loopResources, options.port())
              .handle(acceptor)
              .bind()
              .doOnSuccess(server -> this.server = server)
              .thenReturn(this);
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

  protected HttpServer prepareHttpServer(LoopResources loopResources, int port) {
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
  public String toString() {
    return new StringJoiner(", ", HttpGateway.class.getSimpleName() + "[", "]")
        .add("server=" + server)
        .add("loopResources=" + loopResources)
        .add("corsEnabled=" + corsEnabled)
        .add("corsConfigBuilder=" + corsConfigBuilder)
        .add("options=" + options)
        .toString();
  }
}
