package io.scalecube.gateway.benchmarks.remote;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class RemoteBenchmarkState extends AbstractBenchmarkState<RemoteBenchmarkState> {

  private final InetSocketAddress gatewayAddress;

  /**
   * Constructor for benchmarks state.
   *
   * @param settings benchmarks settings.
   */
  public RemoteBenchmarkState(
      BenchmarkSettings settings,
      BiFunction<InetSocketAddress, LoopResources, Mono<Client>> clientFunction) {
    super(settings, clientFunction);

    String address = settings.find("gatewayAddress", null);
    if (address == null) {
      throw new IllegalArgumentException();
    }
    String[] strings = address.split(":", 2);

    String host = strings[0];
    int port = Integer.parseInt(strings[1]);
    gatewayAddress = InetSocketAddress.createUnresolved(host, port);
  }

  /**
   * Factory function for {@link Client}.
   *
   * @return client
   */
  public Mono<Client> createClient() {
    return createClient(gatewayAddress, clientFunction);
  }
}
