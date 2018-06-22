package io.scalecube.services.benchmarks;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ServicesBenchmarksState extends BenchmarksState<ServicesBenchmarksState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServicesBenchmarksState.class);

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(6);

  private final Object[] services;

  private Microservices seed;
  private Microservices node;

  public ServicesBenchmarksState(BenchmarksSettings settings, Object... services) {
    super(settings);
    this.services = services;
  }

  @Override
  public void beforeAll() {
    seed = Microservices.builder()
        .metrics(settings.registry())
        .startAwait();

    node = Microservices.builder()
        .metrics(settings.registry())
        .seeds(seed.cluster().address())
        .services(services)
        .startAwait();

    LOGGER.info("Seed address: " + seed.cluster().address() +
        ", services address: " + node.serviceAddress() +
        ", seed serviceRegistry: " + seed.serviceRegistry().listServiceReferences());
  }

  @Override
  public void afterAll() {
    if (node != null) {
      try {
        node.shutdown().block(SHUTDOWN_TIMEOUT);
      } catch (Throwable ignore) {
      }
    }

    if (seed != null) {
      try {
        seed.shutdown().block(SHUTDOWN_TIMEOUT);
      } catch (Throwable ignore) {
      }
    }
  }

  public Microservices seed() {
    return seed;
  }

  public <T> T service(Class<T> c) {
    return seed.call().create().api(c);
  }

  public ServiceCall serviceCall() {
    return seed.call().create();
  }
}
