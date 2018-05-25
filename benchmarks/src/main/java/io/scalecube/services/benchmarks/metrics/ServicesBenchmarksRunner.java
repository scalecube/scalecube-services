package io.scalecube.services.benchmarks.metrics;

import java.time.Duration;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) {
    int responseCount = 10;
    Duration reporterPeriodDuration = Duration.ofSeconds(10);
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(nThreads, reporterPeriodDuration).start();


    servicesBenchmarks.oneWay().take(Duration.ofSeconds(20))
        .blockLast();
    servicesBenchmarks.requestOne().take(Duration.ofSeconds(20))
        .blockLast();
    servicesBenchmarks.requestMany(responseCount).take(Duration.ofSeconds(20))
        .blockLast();

    servicesBenchmarks.tearDown();
  }
}


