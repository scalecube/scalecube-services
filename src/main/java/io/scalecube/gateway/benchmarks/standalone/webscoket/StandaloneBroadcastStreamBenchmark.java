package io.scalecube.gateway.benchmarks.standalone.webscoket;

import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;
import io.scalecube.gateway.benchmarks.standalone.StandaloneBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;

public class StandaloneBroadcastStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BroadcastStreamBenchmark.runWith(
        args,
        benchmarkSettings ->
            new StandaloneBenchmarkState(
                benchmarkSettings,
                "ws",
                (address, loopResources) ->
                    Client.onWebsocket(
                        ClientSettings.builder()
                            .address(address)
                            .loopResources(loopResources)
                            .build())));
  }
}
