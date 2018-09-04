package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.codec.RSocketPayloadCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public abstract class AbstractBenchmarkState<T extends AbstractBenchmarkState<T>>
    extends BenchmarkState<T> {

  private LoopResources loopResources;

  public AbstractBenchmarkState(BenchmarkSettings settings) {
    super(settings);
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();
    loopResources = LoopResources.create("worker", 1, true);
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
    ClientMessageCodec<Payload> messageCodec = new RSocketPayloadCodec(headersCodec, dataCodec);

    RSocketClientTransport transport =
        new RSocketClientTransport(settings, messageCodec, loopResources);
    Client client = new Client(transport, messageCodec);

    ClientMessage request = ClientMessage.builder().qualifier("/benchmarks/one").build();
    return client.requestResponse(request).then(Mono.just(client));
  }
}
