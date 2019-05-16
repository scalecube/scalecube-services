package io.scalecube.services.gateway.http;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayLoopResources;
import io.scalecube.services.gateway.GatewayMetrics;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewayTemplate;
import io.scalecube.services.transport.api.Address;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.LoopResources;

public class HttpGateway extends GatewayTemplate {

  private DisposableServer server;
  private LoopResources loopResources;

  public HttpGateway(GatewayOptions options) {
    super(options);
  }

  @Override
  public Mono<Gateway> start() {
    return Mono.defer(
        () -> {
          HttpGatewayAcceptor acceptor = new HttpGatewayAcceptor(options.call(), gatewayMetrics);

          if (options.workerPool() != null) {
            loopResources = new GatewayLoopResources((EventLoopGroup) options.workerPool());
          }

          return httpServer(loopResources, options.port(), null /*metrics*/)
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

  private HttpServer httpServer(
      LoopResources loopResources, int port, GatewayMetrics metrics) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              tcpServer =
                  tcpServer.bootstrap(
                      b ->
                          BootstrapHandlers.updateConfiguration(
                              b,
                              "CORS-bootstrap-handler",
                              (connectionObserver, channel) -> {
                                CorsConfig corsConfig =
                                    CorsConfigBuilder.forAnyOrigin()
                                        .allowedRequestMethods(HttpMethod.POST, HttpMethod.OPTIONS)
                                        .shortCircuit()
                                        .build();
                                CorsHandler corsHandler = new CorsHandler(corsConfig);

                                ChannelPipeline pipeline = channel.pipeline();
                                pipeline.addFirst("CORS", corsHandler);
                              }));
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
              return tcpServer.addressSupplier(() -> new InetSocketAddress(port));
            });
  }
}
