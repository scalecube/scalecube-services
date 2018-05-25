package io.scalecube.services.benchmarks.metrics;

import static io.scalecube.services.benchmarks.BenchmarkService.MESSAGE;

import io.scalecube.services.benchmarks.BenchmarkMessage;
import io.scalecube.services.benchmarks.ServicesBenchmarksState;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ServicesBenchmarks {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServicesBenchmarks.class);

  private final int nThreads;
  private final Duration reporterPeriodDuration;
  private final ServicesBenchmarksState state;
  private final Scheduler scheduler;

  public ServicesBenchmarks(int nThreads, Duration reporterPeriodDuration) {
    this.nThreads = nThreads;
    this.reporterPeriodDuration = reporterPeriodDuration;
    this.scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(nThreads));
    this.state = new ServicesBenchmarksState();
  }

  public synchronized ServicesBenchmarks start() {
    LOGGER.info("###### START");
    state.setup();
    return this;
  }

  public synchronized void tearDown() {
    state.tearDown();
    scheduler.dispose();
    LOGGER.info("###### DONE");
  }

  public synchronized Flux<Void> oneWay() {
    String taskName = "oneWay";
    LOGGER.info("###### " + taskName + ", nThreads=" + nThreads);
    MetricRegistry registry = new MetricRegistry();
    ScheduledReporter reporter = reporter(registry);
    reporter.start(reporterPeriodDuration.toMillis(), TimeUnit.MILLISECONDS);
    Timer timer = registry.timer(taskName + "-timer");

    return Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(scheduler)
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().oneWay(MESSAGE)
              .doOnSuccess(next -> timeContext.stop());
        }))
        .doOnCancel(() -> {
          LOGGER.info("###### " + taskName + " done");
          reporter.report();
          reporter.stop();
        });
  }

  public synchronized Flux<BenchmarkMessage> requestOne() {
    String taskName = "requestOne";
    LOGGER.info("###### " + taskName + ", nThreads=" + nThreads);
    MetricRegistry registry = new MetricRegistry();
    ScheduledReporter reporter = reporter(registry);
    reporter.start(reporterPeriodDuration.toMillis(), TimeUnit.MILLISECONDS);
    Timer timer = registry.timer(taskName + "-timer");

    return Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(scheduler)
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().requestOne(MESSAGE)
              .doOnSuccess(next -> timeContext.stop());
        }))
        .doOnCancel(() -> {
          LOGGER.info("###### " + taskName + " done");
          reporter.report();
          reporter.stop();
        });
  }

  public synchronized Flux<BenchmarkMessage> requestMany(int responseCount) {
    String taskName = "requestMany";
    LOGGER.info("###### " + taskName + ", responseCount=" + responseCount + ", nThreads=" + nThreads);
    BenchmarkMessage message = new BenchmarkMessage(String.valueOf(responseCount));
    MetricRegistry registry = new MetricRegistry();
    ScheduledReporter reporter = reporter(registry);
    reporter.start(reporterPeriodDuration.toMillis(), TimeUnit.MILLISECONDS);
    Timer timer = registry.timer(taskName + "-timer");

    return Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(scheduler)
        .flatMap(i -> {
          Timer.Context timeContext = timer.time();
          return state.service().requestMany(message)
              .doOnTerminate(timeContext::stop);
        })
        .doOnCancel(() -> {
          LOGGER.info("###### " + taskName + " done");
          reporter.report();
          reporter.stop();
        });
  }

  private ScheduledReporter reporter(MetricRegistry registry) {
    return Slf4jReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
  }
}
