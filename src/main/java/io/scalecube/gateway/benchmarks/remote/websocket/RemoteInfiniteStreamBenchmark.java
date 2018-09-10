package io.scalecube.gateway.benchmarks.remote.websocket;

import io.scalecube.gateway.benchmarks.InfiniteStreamBenchmark;
import io.scalecube.gateway.benchmarks.remote.RemoteBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import reactor.core.publisher.Mono;

public class RemoteInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamBenchmark.runWith(
        args,
        benchmarkSettings ->
            new RemoteBenchmarkState(
                benchmarkSettings,
                (address, loopResources) ->
                    Mono.just(
                        Client.onWebsocket(
                            ClientSettings.builder()
                                .address(address)
                                .loopResources(loopResources)
                                .build()))));
  }
}
