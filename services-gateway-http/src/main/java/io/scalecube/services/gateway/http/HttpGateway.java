package io.scalecube.services.gateway.http;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayLoopResources;
import io.scalecube.services.gateway.GatewayMetrics;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewayTemplate;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.function.UnaryOperator;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class HttpGateway extends GatewayTemplate {

  private DisposableServer server;
  private LoopResources loopResources;

  private boolean corsEnabled = false;
  private CorsConfigBuilder corsConfigBuilder =
      CorsConfigBuilder.forAnyOrigin()
          .allowNullOrigin()
          .maxAge(3600)
          .allowedRequestMethods(HttpMethod.POST);

  public HttpGateway(GatewayOptions options) {
    super(options);
  }

  private HttpGateway(HttpGateway other) {
    super(other.options);
    this.server = other.server;
    this.loopResources = other.loopResources;
    this.corsEnabled = other.corsEnabled;
    this.corsConfigBuilder = copy(other.corsConfigBuilder);
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
          ServiceCall serviceCall =
              options.call().requestReleaser(ReferenceCountUtil::safestRelease);
          HttpGatewayAcceptor acceptor = new HttpGatewayAcceptor(serviceCall, gatewayMetrics);

          if (options.workerPool() != null) {
            loopResources = new GatewayLoopResources((EventLoopGroup) options.workerPool());
          }

          return prepareHttpServer(loopResources, options.port(), null /*metrics*/)
              .handle(acceptor)
              .bind()
              .doOnSuccess(server -> this.server = server)
              .thenReturn(this);
        });
  }

  @Override
  public Address address() {
    InetSocketAddress address = server.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public Mono<Void> stop() {
    return shutdownServer(server).then(shutdownLoopResources(loopResources));
  }

  protected HttpServer prepareHttpServer(
      LoopResources loopResources, int port, GatewayMetrics metrics) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              if (loopResources != null) {
                tcpServer = tcpServer.runOn(loopResources);
              }
              if (metrics != null) {
                tcpServer =
                    tcpServer.doOnConnection(
                        connection -> {
                          metrics.incConnection();
                          connection.onDispose(metrics::decConnection);
                        });
              }
              return tcpServer
                  .addressSupplier(() -> new InetSocketAddress(port))
                  .doOnConnection(
                      connection -> {
                        if (corsEnabled) {
                          connection.addHandlerLast(new CorsHandler(corsConfigBuilder.build()));
                        }
                      });
            });
  }
}
