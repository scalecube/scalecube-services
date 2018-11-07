package io.scalecube.services.benchmarks.gateway.standalone;

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

public class StandaloneBenchmarkState extends AbstractBenchmarkState<StandaloneBenchmarkState> {

  private final String gatewayName;

  private Microservices microservices;

  public StandaloneBenchmarkState(
      BenchmarkSettings settings,
      String gatewayName,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    super(settings, clientBuilder);
    this.gatewayName = gatewayName;
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();

    microservices =
        Microservices.builder()
            .services(new BenchmarkServiceImpl())
            .gateway(GatewayConfig.builder("rsws", RSocketGateway.class).build())
            .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).build())
            .gateway(GatewayConfig.builder("http", HttpGateway.class).build())
            .metrics(registry())
            .startAwait();
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (microservices != null) {
      microservices.shutdown().block();
    }
  }

  /**
   * Factory function for {@link Client}.
   *
   * @return client
   */
  public Mono<Client> createClient() {
    return createClient(microservices, gatewayName, clientBuilder);
  }
}
