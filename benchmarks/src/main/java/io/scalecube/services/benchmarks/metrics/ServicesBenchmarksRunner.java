package io.scalecube.services.benchmarks.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.Executors;
import java.util.stream.Stream;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) {
    MetricRegistry registry = new MetricRegistry();
    Scheduler scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(nThreads));

    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(scheduler, registry).start();

    int n = 1_00_000;
    int responseCount = 10;
    int warmup = 1;
    int iteration = 3;

    Stream.<Runnable>of(
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.oneWay(size).blockLast()),
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.oneWayLatch(size, nThreads)),
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestOne(size).blockLast()),
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestOneLatch(size, nThreads)),
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestMany(size, responseCount).blockLast()),
        () -> servicesBenchmarks.run(n, (size) -> servicesBenchmarks.requestManyLatch(size, responseCount, nThreads)))
        .forEach(task -> {
          for (int i = 0; i < warmup + iteration; i++) {
            task.run();
          }
        });

    servicesBenchmarks.tearDown();
  }
}
