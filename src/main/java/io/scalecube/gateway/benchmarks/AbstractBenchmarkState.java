package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import io.scalecube.services.Microservices;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public abstract class AbstractBenchmarkState<T extends AbstractBenchmarkState<T>>
    extends BenchmarkState<T> {

  public static final ClientMessage FIRST_REQUEST =
      ClientMessage.builder().qualifier("/benchmarks/one").build();

  protected LoopResources loopResources;
  protected BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder;

  public AbstractBenchmarkState(
      BenchmarkSettings settings,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    super(settings);
    this.clientBuilder = clientBuilder;
  }

  @Override
  protected void beforeAll() throws Exception {
    super.beforeAll();
    loopResources = LoopResources.create("worker-client-sdk", 1, true);
  }

  @Override
  protected void afterAll() throws Exception {
    super.afterAll();
    if (loopResources != null) {
      loopResources.disposeLater().block();
    }
  }

  public abstract Mono<Client> createClient();

  protected final Mono<Client> createClient(
      Microservices gateway,
      String gatewayName,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    return Mono.defer(() -> createClient(gatewayAddress(gateway, gatewayName), clientBuilder));
  }

  protected final Mono<Client> createClient(
      InetSocketAddress gatewayAddress,
      BiFunction<InetSocketAddress, LoopResources, Client> clientBuilder) {
    return Mono.defer(
        () -> {
          Client client = clientBuilder.apply(gatewayAddress, loopResources);
          return client
              .requestResponse(FIRST_REQUEST)
              .doOnNext(
                  response ->
                      Optional.ofNullable(response.data())
                          .ifPresent(ReferenceCountUtil::safestRelease))
              .then(Mono.just(client));
        });
  }

  private InetSocketAddress gatewayAddress(Microservices gateway, String gatewayName) {
    return gateway
        .gatewayAddresses()
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().name().equals(gatewayName))
        .map(Entry::getValue)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Can't find address for gateway with name '" + gatewayName + "'"));
  }
}
