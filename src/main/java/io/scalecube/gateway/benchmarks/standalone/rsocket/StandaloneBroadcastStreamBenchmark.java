package io.scalecube.gateway.benchmarks.standalone.rsocket;

import io.rsocket.Payload;
import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;
import io.scalecube.gateway.benchmarks.standalone.StandaloneBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import reactor.core.publisher.Mono;

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
                "rsws",
                (address, loopResources) -> {
                  ClientSettings clientSettings = ClientSettings.builder().address(address).build();

                  ClientCodec<Payload> clientCodec =
                      new RSocketClientCodec(
                          HeadersCodec.getInstance(clientSettings.contentType()),
                          DataCodec.getInstance(clientSettings.contentType()));

                  return Mono.just(
                      new Client(
                          new RSocketClientTransport(clientSettings, clientCodec, loopResources),
                          clientCodec));
                }));
  }
}
