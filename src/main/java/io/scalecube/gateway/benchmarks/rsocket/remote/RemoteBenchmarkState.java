package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;

public class RemoteBenchmarkState extends AbstractBenchmarkState<RemoteBenchmarkState> {

  private InetSocketAddress gatewayAddress;

  public RemoteBenchmarkState(BenchmarksSettings settings) {
    super(settings);

    String address = settings.find("gatewayAddress", null);
    if (address == null) {
      throw new IllegalArgumentException();
    }
    String[] strings = address.split(":", 2);

    String host = strings[0];
    int port = Integer.parseInt(strings[1]);
    gatewayAddress = InetSocketAddress.createUnresolved(host, port);
  }

  public Mono<Client> createClient() {
    return createClient(
      ClientSettings.builder()
        .host(gatewayAddress.getHostString())
        .port(gatewayAddress.getPort())
        .build());
  }
}
