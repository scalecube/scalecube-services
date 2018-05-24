package io.scalecube.services.benchmarks.metrics;

import static io.scalecube.services.benchmarks.BenchmarkService.MESSAGE;

import io.scalecube.services.benchmarks.BenchmarkMessage;
import io.scalecube.services.benchmarks.ServicesBenchmarksState;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class ServicesBenchmarks {

  private final MetricRegistry registry;
  private final ServicesBenchmarksState state;
  private final ConsoleReporter reporter;
  private final Scheduler scheduler;

  public ServicesBenchmarks(Scheduler scheduler, MetricRegistry registry) {
    this.registry = registry;
    this.scheduler = scheduler;
    this.state = new ServicesBenchmarksState();
    this.reporter = ConsoleReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
  }

  public synchronized void startAndWarmup(int n) {
    System.out.println("###### START AND WARMUP");
    state.setup();
    reporter.start(1, TimeUnit.DAYS);
    Flux.merge(Flux.range(0, n)
        .subscribeOn(scheduler)
        .map(i -> state.service().requestOne(MESSAGE)))
        .blockLast();
  }

  public synchronized void tearDown() {
    reporter.report();
    reporter.stop();
    state.tearDown();
    scheduler.dispose();
    System.out.println("###### DONE");
  }

  public synchronized void run(int n, Consumer<Integer> consumer) {
    System.out.println("###### TASK IS STARTED");
    long start = System.nanoTime();

    consumer.accept(n);

    long diff = System.nanoTime() - start;
    long nsByOp = diff / n;
    double rps = ((double) n / diff) * 1e9;
    double diffInSec = (double) diff / 1e9;
    System.out.println("###### TASK IS DONE, RESULT: " + diffInSec + "sec, " + nsByOp + "ns/op, " + rps + "op/s");
  }

  public synchronized Flux<Void> oneWay(int n) {
    System.out.println("###### oneWay, n=" + n);
    String taskName = "oneWay";
    Timer timer = registry.timer(taskName + "-timer");
    return Flux.merge(Flux.range(0, n)
        .subscribeOn(scheduler)
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().oneWay(MESSAGE)
              .doOnSuccess(next -> timeContext.stop());
        }));
  }

  public synchronized void oneWayLatch(int n, int nThreads) {
    System.out.println("###### oneWayLatch, n=" + n + ", nThreads=" + nThreads);
    String taskName = "oneWayLatch";
    Timer timer = registry.timer(taskName + "-timer");
    CountDownLatch latch = new CountDownLatch(n);
    int count = n / nThreads;
    for (int j = 0; j < nThreads; j++) {
      scheduler.schedule(() -> {
        for (int i = 0; i < count; i++) {
          Timer.Context timeContext = timer.time();
          state.service().oneWay(MESSAGE)
              .doOnSuccess(next -> {
                timeContext.stop();
                latch.countDown();
              })
              .doOnError(t -> System.exit(124))
              .subscribe();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      System.exit(123);
    }
  }

  public synchronized Flux<BenchmarkMessage> requestOne(int n) {
    System.out.println("###### requestOne, n=" + n);
    String taskName = "requestOne";
    Timer timer = registry.timer(taskName + "-timer");
    return Flux.merge(Flux.range(0, n)
        .subscribeOn(scheduler)
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().requestOne(MESSAGE)
              .doOnSuccess(next -> timeContext.stop());
        }));
  }

  public synchronized void requestOneLatch(int n, int nThreads) {
    System.out.println("###### requestOneLatch, n=" + n + ", nThreads=" + nThreads);
    String taskName = "requestOneLatch";
    Timer timer = registry.timer(taskName + "-timer");
    CountDownLatch latch = new CountDownLatch(n);
    int count = n / nThreads;
    for (int j = 0; j < nThreads; j++) {
      scheduler.schedule(() -> {
        for (int i = 0; i < count; i++) {
          Timer.Context timeContext = timer.time();
          state.service().requestOne(MESSAGE)
              .doOnSuccess(next -> {
                timeContext.stop();
                latch.countDown();
              })
              .doOnError(t -> System.exit(124))
              .subscribe();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      System.exit(123);
    }
  }

  public synchronized Flux<BenchmarkMessage> requestMany(int n, int responseCount) {
    System.out.println("###### requestMany, n=" + n + ", responseCount=" + responseCount);
    String taskName = "requestMany";
    BenchmarkMessage message = new BenchmarkMessage(String.valueOf(responseCount));
    Timer timer = registry.timer(taskName + "-timer");
    return Flux.merge(Flux.range(0, n)
        .subscribeOn(scheduler)
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().requestMany(message)
              .doOnTerminate(timeContext::stop);
        }));
  }

  public synchronized void requestManyLatch(int n, int responseCount, int nThreads) {
    System.out
        .println("###### requestManyLatch, n=" + n + ", responseCount=" + responseCount + ", nThreads=" + nThreads);
    String taskName = "requestManyLatch";
    BenchmarkMessage message = new BenchmarkMessage(String.valueOf(responseCount));
    Timer timer = registry.timer(taskName + "-timer");
    CountDownLatch latch = new CountDownLatch(n);
    int count = n / nThreads;
    for (int j = 0; j < nThreads; j++) {
      scheduler.schedule(() -> {
        for (int i = 0; i < count; i++) {
          Timer.Context timeContext = timer.time();
          state.service().requestMany(message)
              .doOnTerminate(() -> {
                timeContext.stop();
                latch.countDown();
              })
              .doOnError(t -> System.exit(124))
              .subscribe();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      System.exit(123);
    }
  }
}
