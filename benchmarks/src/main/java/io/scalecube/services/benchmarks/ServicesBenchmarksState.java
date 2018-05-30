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
  private final MetricRegistry registry;
  private final ConsoleReporter consoleReporter;
  private final Duration reporterPeriod;
  private final Scheduler scheduler;
  private final CsvReporter csvReporter;

  private Microservices seed;
  private Microservices node;
  private BenchmarkService service;

  public ServicesBenchmarksState(ServicesBenchmarksSettings settings) {
    this.settings = settings;

    registry = new MetricRegistry();

    reporterPeriod = settings.reporterPeriod();

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
  }

  public void setup() {
    seed = Microservices.builder()
        .metrics(registry)
        .build()
        .startAwait();

    node = Microservices.builder()
        .metrics(registry)
        .seeds(seed.cluster().address())
        .services(new BenchmarkServiceImpl())
        .build()
        .startAwait();

    System.err.println("Benchmarks settings: " + settings +
        ", seed address: " + seed.cluster().address() +
        ", services address: " + node.serviceAddress() +
        ", seed serviceRegistry: " + seed.serviceRegistry().listServiceReferences());

    service = seed.call().create().api(BenchmarkService.class);

    consoleReporter.start(reporterPeriod.toMillis(), TimeUnit.MILLISECONDS);
    csvReporter.start(reporterPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void tearDown() {
    consoleReporter.report();
    consoleReporter.stop();

    csvReporter.report();
    csvReporter.stop();

    scheduler.dispose();

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

  public BenchmarkService service() {
    return service;
  }
}
