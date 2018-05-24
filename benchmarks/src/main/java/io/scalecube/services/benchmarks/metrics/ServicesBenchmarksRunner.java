package io.scalecube.services.benchmarks.metrics;

import com.codahale.metrics.MetricRegistry;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) throws Exception {
    MetricRegistry registry = new MetricRegistry();

    int n = 3_000;
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(nThreads, registry);

    servicesBenchmarks.startAndWarmup(n);

    servicesBenchmarks.execute(servicesBenchmarks.fireAndForgetTaskWithBlock(n));
    servicesBenchmarks.execute(servicesBenchmarks.fireAndForgetTaskWithSubscribe(n));
    servicesBenchmarks.execute(servicesBenchmarks.requestOneTaskWithBlock(n));
    servicesBenchmarks.execute(servicesBenchmarks.requestOneTaskWithSubscribe(n));

    servicesBenchmarks.tearDown();
  }


}
