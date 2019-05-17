package io.scalecube.services.gateway.ws;

import io.netty.channel.EventLoopGroup;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayLoopResources;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewayTemplate;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

public class WebsocketGateway extends GatewayTemplate {

  private DisposableServer server;
  private LoopResources loopResources;

  public WebsocketGateway(GatewayOptions options) {
    super(options);
  }

  @Override
  public Mono<Gateway> start() {
    return Mono.defer(
        () -> {
          ServiceCall serviceCall =
              options.call().requestReleaser(ReferenceCountUtil::safestRelease);
          WebsocketGatewayAcceptor acceptor =
              new WebsocketGatewayAcceptor(serviceCall, gatewayMetrics);

          if (options.workerPool() != null) {
            loopResources = new GatewayLoopResources((EventLoopGroup) options.workerPool());
          }

          return prepareHttpServer(loopResources, options.port(), gatewayMetrics)
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
}
