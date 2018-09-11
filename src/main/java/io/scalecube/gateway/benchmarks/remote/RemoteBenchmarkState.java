package io.scalecube.gateway.benchmarks.remote;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class RemoteBenchmarkState extends AbstractBenchmarkState<RemoteBenchmarkState> {

  public static final int WS_PORT = 7070;
  public static final int RS_PORT = 9090;

  private final InetSocketAddress gatewayAddress;

  /**
   * Constructor for benchmarks state.
   *
   * @param settings benchmarks settings.
   */
  public RemoteBenchmarkState(
      BenchmarkSettings settings,
      int gatewayPort,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    super(settings, clientBuilder);
    String gatewayHost = settings.find("gatewayIpAddress", "localhost");
    this.gatewayAddress = InetSocketAddress.createUnresolved(gatewayHost, gatewayPort);
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
