package io.scalecube.services.benchmarks.metrics;

import com.codahale.metrics.MetricRegistry;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarksRunner {

  private static final int nThreads = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) throws Exception {
    MetricRegistry registry = new MetricRegistry();

    int n = 1_000_000;
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(nThreads, registry);

    servicesBenchmarks.startAndWarmup(500);

//    servicesBenchmarks.execute(servicesBenchmarks.fireAndForgetTaskWithBlock(n));
//    servicesBenchmarks.execute(servicesBenchmarks.fireAndForgetTaskWithSubscribe(n));
//    servicesBenchmarks.execute(servicesBenchmarks.requestOneTaskWithBlock(n));
//    servicesBenchmarks.execute(servicesBenchmarks.requestOneTaskWithSubscribe(n));

//    servicesBenchmarks.requestResponse(n).subscribeOn(Schedulers.elastic()).block();
    long start = System.nanoTime();
    servicesBenchmarks.requestResponse(n).blockLast();
    long l = (System.nanoTime() - start) / n ;
    System.out.println("#### RESULT: " +  l + "ns/op");
    servicesBenchmarks.tearDown();
  }


}
