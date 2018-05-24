package io.scalecube.services.benchmarks.metrics;

import static io.scalecube.services.benchmarks.BenchmarkService.MESSAGE;

import io.scalecube.services.benchmarks.ServicesBenchmarksState;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ServicesBenchmarks {

  private final int nThreads;
  private final MetricRegistry registry;
  private final ServicesBenchmarksState state;
  private final ExecutorService executorService;

  public ServicesBenchmarks(int nThreads, MetricRegistry registry) {
    this.nThreads = nThreads;
    this.registry = registry;
    this.state = new ServicesBenchmarksState();
    this.executorService = Executors.newFixedThreadPool(nThreads);
  }

  public void execute(Runnable task) {
    try {
      System.out.println("###### STARTING EXECUTION");
      state.setup();
      ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
      reporter.start(1, TimeUnit.DAYS);
      CompletableFuture[] futures = new CompletableFuture[nThreads];
      IntStream.range(0, nThreads)
          .forEach(i -> futures[i] = CompletableFuture.runAsync(task, executorService));
      CompletableFuture.allOf(futures).join();
      reporter.report();
      reporter.stop();
    } finally {
      state.tearDown();
    }
    System.out.println("###### DONE");
  }

  public void tearDown() throws Exception {
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  public Runnable fireAndForgetTaskWithSubscribe(int n) {
    Timer timer = registry.timer("fireAndForgetTaskWithSubscribe-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| TASK IS STARTED");
      CountDownLatch latch = new CountDownLatch(n);
      for (int i = 0; i < n; i++) {
        Timer.Context timeContext = timer.time();
        state.service().fireAndForget(MESSAGE)
            .doOnSuccess(v -> {
              latch.countDown();
              timeContext.stop();
            })
            .subscribe();
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
      System.out.println("###### |" + Thread.currentThread().getName() + "| TASK IS DONE");
    };
  }

  public Runnable fireAndForgetTaskWithBlock(int n) {
    Timer timer = registry.timer("fireAndForgetTaskWithBlock-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| TASK IS STARTED");
      for (int i = 0; i < n; i++) {
        Timer.Context timeContext = timer.time();
        state.service().fireAndForget(MESSAGE)
            .doOnSuccess(v -> timeContext.stop())
            .block();
      }
      System.out.println("###### |" + Thread.currentThread().getName() + "| TASK IS DONE");
    };
  }
}
