package io.scalecube.services.benchmarks.gateway.distributed;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.services.Microservices;
import io.scalecube.services.benchmarks.gateway.AbstractBenchmarkState;
import io.scalecube.services.examples.BenchmarkServiceImpl;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.rsocket.RSocketGateway;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

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
            .gateway(GatewayConfig.builder("rsws", RSocketGateway.class).build())
            .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).build())
            .gateway(GatewayConfig.builder("http", HttpGateway.class).build())
            .metrics(registry())
            .startAwait();

    services =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .transport(options -> options.numOfThreads(Runtime.getRuntime().availableProcessors()))
            .services(new BenchmarkServiceImpl())
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
