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
  private final ConsoleReporter reporter;

  public ServicesBenchmarks(int nThreads, MetricRegistry registry) {
    this.nThreads = nThreads;
    this.registry = registry;
    this.state = new ServicesBenchmarksState();
    this.executorService = Executors.newFixedThreadPool(nThreads);
    reporter = ConsoleReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
  }

  public synchronized void execute(Runnable task) {
    CompletableFuture[] futures = new CompletableFuture[nThreads];
    IntStream.range(0, nThreads)
        .forEach(i -> futures[i] = CompletableFuture.runAsync(task, executorService));
    CompletableFuture.allOf(futures).join();
  }

  public synchronized void tearDown() throws Exception {
    reporter.report();
    reporter.stop();
    state.tearDown();
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
    System.out.println("###### DONE");
  }

  public synchronized void startAndWarmup(int n) {
    System.out.println("###### START AND WARMUP");
    state.setup();
    reporter.start(1, TimeUnit.DAYS);
    execute(() -> {
      for (int i = 0; i < n; i++) {
        state.service().fireAndForget(MESSAGE)
            .block();
      }
    });
  }

  public Runnable fireAndForgetTaskWithSubscribe(int n) {
    String taskName = "fireAndForgetTaskWithSubscribe";
    Timer timer = registry.timer(taskName + "-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS STARTED");
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
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS DONE");
    };
  }

  public Runnable fireAndForgetTaskWithBlock(int n) {
    String taskName = "fireAndForgetTaskWithBlock";
    Timer timer = registry.timer(taskName + "-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS STARTED");
      for (int i = 0; i < n; i++) {
        Timer.Context timeContext = timer.time();
        state.service().fireAndForget(MESSAGE)
            .doOnSuccess(v -> timeContext.stop())
            .block();
      }
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS DONE");
    };
  }

  public Runnable requestOneTaskWithSubscribe(int n) {
    String taskName = "requestOneTaskWithSubscribe";
    Timer timer = registry.timer(taskName + "-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS STARTED");
      CountDownLatch latch = new CountDownLatch(n);
      for (int i = 0; i < n; i++) {
        Timer.Context timeContext = timer.time();
        state.service().requestOne(MESSAGE)
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
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS DONE");
    };
  }

  public Runnable requestOneTaskWithBlock(int n) {
    String taskName = "requestOneTaskWithBlock";
    Timer timer = registry.timer(taskName + "-timer");
    return () -> {
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS STARTED");
      for (int i = 0; i < n; i++) {
        Timer.Context timeContext = timer.time();
        state.service().requestOne(MESSAGE)
            .doOnSuccess(v -> timeContext.stop())
            .block();
      }
      System.out.println("###### |" + Thread.currentThread().getName() + "| " + taskName + " TASK IS DONE");
    };
  }
}
