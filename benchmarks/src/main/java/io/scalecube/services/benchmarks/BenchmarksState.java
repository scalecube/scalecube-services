package io.scalecube.services.benchmarks;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.LongStream;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class BenchmarksState<T extends BenchmarksState<T>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarksState.class);

  protected final BenchmarksSettings settings;

  private ConsoleReporter consoleReporter;
  private Scheduler scheduler;
  private CsvReporter csvReporter;

  private final AtomicBoolean started = new AtomicBoolean();

  public BenchmarksState(BenchmarksSettings settings) {
    this.settings = settings;
  }

  protected void beforeAll() {
    // NOP
  }

  protected void afterAll() {
    // NOP
  }

  public final void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("BenchmarksState is already started");
    }

    LOGGER.info("Benchmarks settings: " + settings);

    consoleReporter = ConsoleReporter.forRegistry(settings.registry())
        .outputTo(System.out)
        .convertDurationsTo(settings.durationUnit())
        .convertRatesTo(settings.rateUnit())
        .build();

    csvReporter = CsvReporter.forRegistry(settings.registry())
        .convertDurationsTo(settings.durationUnit())
        .convertRatesTo(settings.rateUnit())
        .build(settings.csvReporterDirectory());

    scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(settings.nThreads()));

    beforeAll();

    consoleReporter.start(settings.reporterPeriod().toMillis(), TimeUnit.MILLISECONDS);
    csvReporter.start(1, TimeUnit.DAYS);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (started.get()) {
        csvReporter.report();
        consoleReporter.report();
      }
    }));

  }

  public final void shutdown() {
    if (!started.compareAndSet(true, false)) {
      throw new IllegalStateException("BenchmarksState is not started");
    }

    if (consoleReporter != null) {
      consoleReporter.report();
      consoleReporter.stop();
    }

    if (csvReporter != null) {
      csvReporter.report();
      csvReporter.stop();
    }

    if (scheduler != null) {
      scheduler.dispose();
    }

    afterAll();
  }

  public Scheduler scheduler() {
    return scheduler;
  }

  public Timer timer(String name) {
    return settings.registry().timer(settings.taskName() + "-" + name);
  }

  public Meter meter(String name) {
    return settings.registry().meter(settings.taskName() + "-" + name);
  }

  public Histogram histogram(String name) {
    return settings.registry().histogram(settings.taskName() + "-" + name);
  }

  public final void blockLastObject(Function<T, Function<Long, Object>> func) {
    try {
      start();
      // noinspection unchecked
      Function<Long, Object> func1 = func.apply((T) this);
      Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
          .publishOn(scheduler())
          .map(func1))
          .take(settings.executionTaskTime())
          .blockLast();
    } finally {
      shutdown();
    }
  }

  public final void blockLastPublisher(Function<T, Function<Long, Publisher<?>>> func) {
    try {
      start();
      // noinspection unchecked
      Function<Long, Publisher<?>> func1 = func.apply((T) this);
      Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
          .publishOn(scheduler())
          .map(func1))
          .take(settings.executionTaskTime())
          .blockLast();
    } finally {
      shutdown();
    }
  }

}
