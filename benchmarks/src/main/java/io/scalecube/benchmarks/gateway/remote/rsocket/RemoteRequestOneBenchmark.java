package io.scalecube.benchmarks.gateway.remote.rsocket;

import static io.scalecube.benchmarks.gateway.remote.RemoteBenchmarkState.RS_PORT;

import io.scalecube.benchmarks.gateway.RequestOneScenario;
import io.scalecube.benchmarks.gateway.remote.RemoteBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;

public class RemoteRequestOneBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneScenario.runWith(
        args,
        benchmarkSettings ->
            new RemoteBenchmarkState(
                benchmarkSettings,
                RS_PORT,
                (address, loopResources) ->
                    Client.onRSocket(
                        ClientSettings.builder()
                            .address(address)
                            .loopResources(loopResources)
                            .build())));
  }
}
