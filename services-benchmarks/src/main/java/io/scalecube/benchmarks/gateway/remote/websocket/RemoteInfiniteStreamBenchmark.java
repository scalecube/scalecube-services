package io.scalecube.benchmarks.gateway.remote.websocket;

import static io.scalecube.benchmarks.gateway.remote.RemoteBenchmarkState.WS_PORT;

import io.scalecube.benchmarks.gateway.InfiniteStreamScenario;
import io.scalecube.benchmarks.gateway.remote.RemoteBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;

public class RemoteInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamScenario.runWith(
        args,
        benchmarkSettings ->
            new RemoteBenchmarkState(
                benchmarkSettings,
                WS_PORT,
                (address, loopResources) ->
                    Client.onWebsocket(
                        ClientSettings.builder()
                            .address(address)
                            .loopResources(loopResources)
                            .build())));
  }
}
