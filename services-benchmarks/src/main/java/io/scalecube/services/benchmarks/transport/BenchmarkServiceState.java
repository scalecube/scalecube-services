package io.scalecube.services.benchmarks.transport;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.benchmarks.ServiceTransports;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class BenchmarkServiceState extends BenchmarkState<BenchmarkServiceState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkServiceState.class);

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(6);

  private final Object[] services;

  private Microservices seed;
  private Microservices node;

  public BenchmarkServiceState(BenchmarkSettings settings, Object... services) {
    super(settings);
    this.services = services;
  }

  @Override
  public void beforeAll() {
    seed =
        Microservices.builder()
            .metrics(registry())
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .startAwait();

    Address seedAddress = seed.discovery().address();

    node =
        Microservices.builder()
            .metrics(registry())
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(seedAddress)))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(services)
            .startAwait();

    LOGGER.info(
        "Seed address: "
            + seed.discovery().address()
            + ", services address: "
            + node.serviceAddress());
  }

  @Override
  public void afterAll() {
    try {
      Mono.when(node.shutdown(), seed.shutdown()).block(SHUTDOWN_TIMEOUT);
    } catch (Throwable ignore) {
      // ignore
    }
  }

  public Microservices seed() {
    return seed;
  }

  public <T> T api(Class<T> c) {
    return call().api(c);
  }

  public ServiceCall call() {
    return seed.call();
  }
}
