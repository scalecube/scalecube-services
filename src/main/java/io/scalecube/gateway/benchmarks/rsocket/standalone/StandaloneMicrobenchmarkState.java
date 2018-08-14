package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.benchmarks.example.ExampleServiceImpl;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;
import io.scalecube.services.Microservices;

import reactor.core.publisher.Mono;

public class StandaloneMicrobenchmarkState extends AbstractBenchmarkState<StandaloneMicrobenchmarkState> {

  private RSocketWebsocketServer gateway;

  public StandaloneMicrobenchmarkState(BenchmarksSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();

    Microservices microservices = Microservices.builder()
        .services(new ExampleServiceImpl())
        .startAwait();

    gateway = new RSocketWebsocketServer(microservices);
    gateway.start();
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (gateway != null) {
      gateway.stop();
    }
  }

  public Mono<Client> createClient() {
    return createClient(ClientSettings.builder()
        .host(gateway.address().getHostString())
        .port(gateway.address().getPort())
        .build());
  }
}
