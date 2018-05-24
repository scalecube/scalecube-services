package io.scalecube.services.benchmarks.metrics;

import io.scalecube.services.benchmarks.BenchmarkMessage;

import com.codahale.metrics.MetricRegistry;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();
  private static final BenchmarkMessage MESSAGE = new BenchmarkMessage("benchmarkMessage");

  public static void main(String[] args) throws Exception {
    MetricRegistry registry = new MetricRegistry();

    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(nThreads, (int) 1e6, registry);

    servicesBenchmarks.fireAndForget();
  }


}
