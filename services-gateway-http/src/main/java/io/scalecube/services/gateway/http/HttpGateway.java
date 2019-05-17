package io.scalecube.services.gateway.http;

import io.netty.channel.EventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayLoopResources;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.GatewayTemplate;
import io.scalecube.services.transport.api.Address;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
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
          ServiceCall serviceCall = options.call().requestReleaser(ReferenceCountUtil::safeRelease);
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
}
