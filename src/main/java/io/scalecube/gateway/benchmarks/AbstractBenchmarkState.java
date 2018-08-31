package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public abstract class AbstractBenchmarkState<T extends AbstractBenchmarkState<T>>
    extends BenchmarksState<T> {

  private LoopResources loopResources;

  public AbstractBenchmarkState(BenchmarksSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();
    loopResources = LoopResources.create("client-event-loop");
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (loopResources != null) {
      loopResources.disposeLater().block();
    }
  }

  public abstract Mono<Client> createClient();

  protected final Mono<Client> createClient(ClientSettings settings) {
    HeadersCodec headersCodec = HeadersCodec.getInstance(settings.contentType());
    DataCodec dataCodec = DataCodec.getInstance(settings.contentType());
    ClientMessageCodec messageCodec = new ClientMessageCodec(headersCodec, dataCodec);

    RSocketClientTransport transport =
        new RSocketClientTransport(settings, messageCodec, loopResources);
    Client client = new Client(transport, messageCodec);

    ClientMessage request = ClientMessage.builder().qualifier("/benchmarks/one").build();
    return client.requestResponse(request).then(Mono.just(client));
  }
}
