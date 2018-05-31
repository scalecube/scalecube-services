package io.scalecube.services.benchmarks;

import io.scalecube.services.Microservices;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarksState {

  private final ServicesBenchmarksSettings settings;
  private final Object[] services;

  private MetricRegistry registry;
  private ConsoleReporter consoleReporter;
  private Scheduler scheduler;
  private CsvReporter csvReporter;

  private Microservices seed;
  private Microservices node;

  public ServicesBenchmarksState(ServicesBenchmarksSettings settings, Object... services) {
    this.settings = settings;
    this.services = services;
  }

  public void setup() {
    registry = new MetricRegistry();

    seed = Microservices.builder()
        .metrics(registry)
        .build()
        .startAwait();

    node = Microservices.builder()
        .metrics(registry)
        .seeds(seed.cluster().address())
        .services(services)
        .build()
        .startAwait();

    System.err.println("Benchmarks settings: " + settings +
        ", seed address: " + seed.cluster().address() +
        ", services address: " + node.serviceAddress() +
        ", seed serviceRegistry: " + seed.serviceRegistry().listServiceReferences());

    consoleReporter = ConsoleReporter.forRegistry(registry)
        .outputTo(System.err)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build();

    csvReporter = CsvReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(settings.csvReporterDirectory());

    scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(settings.nThreads()));

    Duration reporterPeriod = settings.reporterPeriod();
    consoleReporter.start(reporterPeriod.toMillis(), TimeUnit.MILLISECONDS);
    csvReporter.start(reporterPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void tearDown() {
    if (consoleReporter != null) {
      consoleReporter.report();
      consoleReporter.stop();
    }

    if (csvReporter != null) {
      csvReporter.report();
      csvReporter.stop();
    }

    if (scheduler != null) {
      scheduler.dispose();
    }

    if (node != null) {
      node.shutdown().block();
    }

    if (seed != null) {
      seed.shutdown().block();
    }
  }

  public MetricRegistry registry() {
    return registry;
  }

  public Scheduler scheduler() {
    return scheduler;
  }

  public Microservices seed() {
    return seed;
  }

  public <T> T service(Class<T> c) {
    return seed.call().create().api(c);
  }
}
