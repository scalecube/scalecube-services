package io.scalecube.services.benchmarks.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.Executors;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) {
    MetricRegistry registry = new MetricRegistry();

    Scheduler scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(nThreads));

    int n = 1_00_000;
    int responseCount = 10;
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(scheduler, registry);

    servicesBenchmarks.startAndWarmup(n);

    System.out.println("###### oneWay, n=" + n);
    servicesBenchmarks.run(n, (size) -> servicesBenchmarks.oneWay(size).blockLast());
    System.out.println("###### requestOne, n=" + n);
    servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestOne(size).blockLast());
    System.out.println("###### requestMany, n=" + n + ", responseCount=" + responseCount);
    servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestMany(size, responseCount).blockLast());

    servicesBenchmarks.tearDown();
  }


}
