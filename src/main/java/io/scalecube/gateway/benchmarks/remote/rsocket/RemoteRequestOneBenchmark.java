package io.scalecube.gateway.benchmarks.remote.rsocket;

import io.rsocket.Payload;
import io.scalecube.gateway.benchmarks.RequestOneBenchmark;
import io.scalecube.gateway.benchmarks.remote.RemoteBenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import reactor.core.publisher.Mono;

public class RemoteRequestOneBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneBenchmark.runWith(
        args,
        benchmarkSettings ->
            new RemoteBenchmarkState(
                benchmarkSettings,
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
