package io.scalecube.gateway.benchmarks.standalone.rsocket;

import io.scalecube.gateway.benchmarks.InfiniteStreamBenchmark;
import io.scalecube.gateway.benchmarks.standalone.StandaloneBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import reactor.core.publisher.Mono;

public class StandaloneInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamBenchmark.runWith(
        args,
        benchmarkSettings ->
            new StandaloneBenchmarkState(
                benchmarkSettings,
                "rsws",
                (address, loopResources) ->
                    Mono.just(
                        Client.onRSocket(
                            ClientSettings.builder()
                                .address(address)
                                .loopResources(loopResources)
                                .build()))));
  }
}
