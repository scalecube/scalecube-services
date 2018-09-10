package io.scalecube.gateway.benchmarks.distributed.websocket;

import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;
import io.scalecube.gateway.benchmarks.distributed.DistributedBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import reactor.core.publisher.Mono;

public class DistributedBroadcastStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BroadcastStreamBenchmark.runWith(
        args,
        benchmarkSettings ->
            new DistributedBenchmarkState(
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
