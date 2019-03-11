package io.scalecube.services.benchmarks.gateway.remote;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.services.benchmarks.gateway.AbstractBenchmarkState;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.transport.api.Address;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

public class RemoteBenchmarkState extends AbstractBenchmarkState<RemoteBenchmarkState> {

  public static final int WS_PORT = 7070;
  public static final int RS_PORT = 9090;
  public static final int HTTP_PORT = 8080;

  private final Address gatewayAddress;

  /**
   * Constructor for benchmarks state.
   *
   * @param settings benchmarks settings.
   */
  public RemoteBenchmarkState(
      BenchmarkSettings settings,
      int gatewayPort,
      BiFunction<Address, LoopResources, Client> clientBuilder) {
    super(settings, clientBuilder);
    gatewayAddress = Address.create(settings.find("gatewayHost", "localhost"), gatewayPort);
  }

  /**
   * Factory function for {@link Client}.
   *
   * @return client
   */
  public Mono<Client> createClient() {
    return createClient(gatewayAddress, clientBuilder);
  }
}
