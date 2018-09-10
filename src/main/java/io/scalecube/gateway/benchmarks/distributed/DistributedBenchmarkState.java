package io.scalecube.gateway.benchmarks.distributed;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.benchmarks.BenchmarksServiceImpl;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.gateway.websocket.WebsocketGateway;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.GatewayConfig;
import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class DistributedBenchmarkState extends AbstractBenchmarkState<DistributedBenchmarkState> {

  private final String gatewayName;

  private Microservices services;
  private Microservices gateway;

  public DistributedBenchmarkState(
      BenchmarkSettings settings,
      String gatewayName,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    super(settings, clientBuilder);
    this.gatewayName = gatewayName;
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();

    gateway =
        Microservices.builder()
            .gateway(GatewayConfig.builder("rsws", RSocketWebsocketGateway.class).build())
            .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).build())
            .metrics(registry())
            .startAwait();

    services =
        Microservices.builder()
            .seeds(gateway.discovery().address())
            .services(new BenchmarksServiceImpl())
            .startAwait();
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (services != null) {
      services.shutdown().block();
    }
    if (gateway != null) {
      gateway.shutdown().block();
    }
  }

  @Override
  public Mono<Client> createClient() {
    return createClient(gateway, gatewayName, clientBuilder);
  }
}
