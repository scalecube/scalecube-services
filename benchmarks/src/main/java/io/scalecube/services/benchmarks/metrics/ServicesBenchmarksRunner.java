package io.scalecube.services.benchmarks.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.Executors;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) throws Exception {
    MetricRegistry registry = new MetricRegistry();

    Scheduler scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(nThreads));

    int n = 1_000_000;
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(scheduler, registry);

    servicesBenchmarks.startAndWarmup(n);

    servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestResponse(size).blockLast());

    servicesBenchmarks.tearDown();
  }


}
