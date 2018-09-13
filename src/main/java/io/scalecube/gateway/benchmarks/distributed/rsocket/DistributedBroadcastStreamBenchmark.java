package io.scalecube.gateway.benchmarks.distributed.rsocket;

import io.scalecube.gateway.benchmarks.BroadcastStreamScenario;
import io.scalecube.gateway.benchmarks.distributed.DistributedBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;

public class DistributedBroadcastStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BroadcastStreamScenario.runWith(
        args,
        benchmarkSettings ->
            new DistributedBenchmarkState(
                benchmarkSettings,
                "rsws",
                (address, loopResources) ->
                    Client.onRSocket(
                        ClientSettings.builder()
                            .address(address)
                            .loopResources(loopResources)
                            .build())));
  }
}
