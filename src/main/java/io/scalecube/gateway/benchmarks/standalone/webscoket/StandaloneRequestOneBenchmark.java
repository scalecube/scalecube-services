package io.scalecube.gateway.benchmarks.standalone.webscoket;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;
import io.scalecube.gateway.benchmarks.standalone.StandaloneBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import reactor.core.publisher.Mono;

public class StandaloneRequestOneBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneBenchmark.runWith(
        args,
        benchmarkSettings ->
            new StandaloneBenchmarkState(
                benchmarkSettings,
                "ws",
                (address, loopResources) ->
                    Mono.just(
                        Client.onWebsocket(
                            ClientSettings.builder()
                                .address(address)
                                .loopResources(loopResources)
                                .build()))));
  }
}
