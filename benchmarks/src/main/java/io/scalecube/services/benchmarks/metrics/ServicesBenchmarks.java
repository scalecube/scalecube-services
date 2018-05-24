package io.scalecube.services.benchmarks.metrics;

import io.scalecube.services.benchmarks.BenchmarkMessage;
import io.scalecube.services.benchmarks.ServicesBenchmarksState;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ServicesBenchmarks implements Runnable {

  private static final BenchmarkMessage MESSAGE = new BenchmarkMessage("benchmarkMessage");

  private final int nThreads;
  private final int n;
  private final MetricRegistry registry;

  private final ServicesBenchmarksState state;
  private final ExecutorService executorService;
  private final Timer fireAndForgetTimer;

  public ServicesBenchmarks(int nThreads, int n, MetricRegistry registry) {
    this.nThreads = nThreads;
    this.n = n;
    this.registry = registry;
    this.fireAndForgetTimer = registry.timer("fireAndForgetTimer");
    this.state = new ServicesBenchmarksState();
    this.executorService = Executors.newFixedThreadPool(nThreads);
  }

  public void fireAndForget() throws Exception {
    try {
      System.out.println("###### Starting");
      state.setup();
      ConsoleReporter reporter =
          ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS).build();

      reporter.start(10, TimeUnit.SECONDS);
      CompletableFuture[] futures = new CompletableFuture[nThreads];
      IntStream.range(0, nThreads).forEach(i -> futures[i] = CompletableFuture.runAsync(this, executorService));
      CompletableFuture.allOf(futures).join();
      reporter.report();
      reporter.stop();
    } finally {
      state.tearDown();
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  @Override
  public void run() {
    // CountDownLatch latch = new CountDownLatch(n);
    for (int i = 0; i < n; i++) {
      Timer.Context timeContext = fireAndForgetTimer.time();
      state.service().fireAndForget(MESSAGE)
          .doOnSuccess(v -> {
            // latch.countDown();
            timeContext.stop();
          })
          .block();
      // .subscribe();
    }
    // try {
    // latch.await();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    System.out.println("###### DONE");
  }
}
