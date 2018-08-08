package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

import java.net.InetSocketAddress;

public class RSWSBenchmarkState extends BenchmarksState<RSWSBenchmarkState> {

  private static final String CONTENT_TYPE = "application/json";

  private LoopResources loopResources;
  private InetSocketAddress gwAddress;

  public RSWSBenchmarkState(BenchmarksSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() {
    loopResources = LoopResources.create("rsws-loop");
    String address = settings.find("gw_address", null);
    if (address == null) {
      throw new IllegalArgumentException();
    }
    String[] strings = address.split(":");
    gwAddress = InetSocketAddress.createUnresolved(strings[0], Integer.parseInt(strings[1]));
  }

  @Override
  protected void afterAll() {
    if (loopResources != null) {
      loopResources.disposeLater().block();
    }
  }

  public Mono<Client> createClient() {
    ClientSettings clientSettings = ClientSettings.builder()
        .host(gwAddress.getHostString())
        .port(gwAddress.getPort())
        .build();

    HeadersCodec headersCodec = HeadersCodec.getInstance(CONTENT_TYPE);
    DataCodec dataCodec = DataCodec.getInstance(CONTENT_TYPE);
    ClientMessageCodec messageCodec = new ClientMessageCodec(headersCodec, dataCodec);

    RSocketClientTransport transport = new RSocketClientTransport(clientSettings, messageCodec, loopResources);
    Client client = new Client(transport, messageCodec);

    return client.forService(ExampleService.class).one("hello").then(Mono.just(client));
  }

}
