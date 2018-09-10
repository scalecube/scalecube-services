package io.scalecube.gateway.benchmarks.distributed.rsocket;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;
import io.scalecube.gateway.benchmarks.distributed.DistributedBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import reactor.core.publisher.Mono;

public class DistributedRequestOneBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneBenchmark.runWith(
        args,
        benchmarkSettings ->
            new DistributedBenchmarkState(
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
