package io.scalecube.services.benchmarks.gateway;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import io.scalecube.services.gateway.clientsdk.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.resources.LoopResources;

public abstract class AbstractBenchmarkState<T extends AbstractBenchmarkState<T>>
    extends BenchmarkState<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBenchmarkState.class);

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
    int workerCount = Runtime.getRuntime().availableProcessors();
    loopResources = LoopResources.create("worker-client-sdk", workerCount, true);
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
              .log("benchmark-client-first-request", Level.INFO, false, SignalType.ON_NEXT)
              .doOnNext(
                  response ->
                      Optional.ofNullable(response.data())
                          .ifPresent(ReferenceCountUtil::safestRelease))
              .then(Mono.just(client))
              .doOnNext(c -> LOGGER.info("benchmark-client: {}", c));
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
