package io.scalecube.gateway.benchmarks.distributed.websocket;

import io.scalecube.gateway.benchmarks.InfiniteStreamBenchmark;
import io.scalecube.gateway.benchmarks.distributed.DistributedBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientCodec;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import reactor.core.publisher.Mono;

public class DistributedInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamBenchmark.runWith(
        args,
        benchmarkSettings ->
            new DistributedBenchmarkState(
                benchmarkSettings,
                "ws",
                (address, loopResources) -> {
                  ClientSettings clientSettings = ClientSettings.builder().address(address).build();

                  WebsocketClientCodec clientCodec =
                      new WebsocketClientCodec(DataCodec.getInstance(clientSettings.contentType()));

                  return Mono.just(
                      new Client(
                          new WebsocketClientTransport(clientSettings, clientCodec, loopResources),
                          clientCodec));
                }));
  }
}
