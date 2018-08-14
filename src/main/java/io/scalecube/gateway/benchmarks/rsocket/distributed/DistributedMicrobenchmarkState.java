package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.benchmarks.example.ExampleServiceImpl;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;
import io.scalecube.services.Microservices;

import reactor.core.publisher.Mono;

public class DistributedMicrobenchmarkState extends AbstractBenchmarkState<DistributedMicrobenchmarkState> {

  private RSocketWebsocketServer gateway;
  private Microservices services;

  public DistributedMicrobenchmarkState(BenchmarksSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();

    services = Microservices.builder()
        .services(new ExampleServiceImpl())
        .startAwait();

    Microservices microservices = Microservices.builder()
        .seeds(services.discovery().address())
        .startAwait();

    gateway = new RSocketWebsocketServer(microservices);
    gateway.start();
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (services != null) {
      services.shutdown().block();
    }
    if (gateway != null) {
      gateway.stop();
    }
  }

  @Override
  public Mono<Client> createClient() {
    return createClient(ClientSettings.builder()
        .host(gateway.address().getHostString())
        .port(gateway.address().getPort())
        .build());
  }
}
