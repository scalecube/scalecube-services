package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.services.Microservices;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;

public abstract class AbstractBenchmarkState<T extends AbstractBenchmarkState<T>>
    extends BenchmarkState<T> {

  public static final ClientMessage FIRST_REQUEST =
      ClientMessage.builder().qualifier("/benchmarks/one").build();

  protected LoopResources loopResources;
  protected BiFunction<InetSocketAddress, LoopResources, Mono<Client>> clientFunction;

  public AbstractBenchmarkState(
      BenchmarkSettings settings,
      BiFunction<InetSocketAddress, LoopResources, Mono<Client>> clientFunction) {
    super(settings);
    this.clientFunction = clientFunction;
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

  protected final Mono<Client> createClient(
      Microservices gateway,
      String gatewayName,
      BiFunction<InetSocketAddress, LoopResources, Mono<Client>> clientFunction) {
    return Mono.defer(() -> createClient(gatewayAddress(gateway, gatewayName), clientFunction));
  }

  protected final Mono<Client> createClient(
      InetSocketAddress gatewayAddress,
      BiFunction<InetSocketAddress, LoopResources, Mono<Client>> clientFunction) {
    return Mono.defer(
        () -> {
          Mono<Client> clientMono = clientFunction.apply(gatewayAddress, loopResources);
          return clientMono
              .flatMap(client -> client.requestResponse(FIRST_REQUEST))
              .then(clientMono);
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
        .orElseThrow(() -> new IllegalArgumentException(gatewayName));
  }
}
