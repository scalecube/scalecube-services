package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import io.scalecube.gateway.benchmarks.example.ExampleServiceImpl;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;
import io.scalecube.services.Microservices;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public class RSWSStandaloneMicrobenchmarkState extends BenchmarksState<RSWSStandaloneMicrobenchmarkState> {

  private static final String CONTENT_TYPE = "application/json";

  private LoopResources loopResources;
  private RSocketWebsocketServer server;

  public RSWSStandaloneMicrobenchmarkState(BenchmarksSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() {
    loopResources = LoopResources.create("rsws-loop");

    Microservices microservices = Microservices.builder()
        .services(new ExampleServiceImpl())
        .startAwait();

    server = new RSocketWebsocketServer(microservices);
    server.start();
  }

  @Override
  protected void afterAll() {
    if (loopResources != null) {
      loopResources.disposeLater().block();
    }
    if (server != null) {
      server.stop();
    }
  }

  public Mono<Client> createClient() {
    ClientSettings clientSettings = ClientSettings.builder()
        .host(server.address().getHostString())
        .port(server.address().getPort())
        .build();

    HeadersCodec headersCodec = HeadersCodec.getInstance(CONTENT_TYPE);
    DataCodec dataCodec = DataCodec.getInstance(CONTENT_TYPE);
    ClientMessageCodec messageCodec = new ClientMessageCodec(headersCodec, dataCodec);

    RSocketClientTransport transport = new RSocketClientTransport(clientSettings, messageCodec, loopResources);
    Client client = new Client(transport, messageCodec);

    return client.forService(ExampleService.class).one("hello").then(Mono.just(client));
  }

}
