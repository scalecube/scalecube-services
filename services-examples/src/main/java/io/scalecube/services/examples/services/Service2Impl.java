package io.scalecube.services.examples.services;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;

class Service2Impl implements Service2 {

  private static final long SLEEP_PERIOD_NS = 10000;

  @Override
  public Mono<String> oneDelay(long interval) {
    return Mono.<String>create(sink -> doWork(sink, interval))
        .subscribeOn(Schedulers.parallel())
        .log("oneDelay    |");
  }

  private void doWork(MonoSink<String> sink, long interval) {
    AtomicBoolean isActive = new AtomicBoolean(true);
    sink.onCancel(() -> isActive.set(false));
    sink.onDispose(() -> isActive.set(false));

    long started = System.currentTimeMillis();

    sink.onRequest(
        r -> {
          while (isActive.get()) {
            long now = System.currentTimeMillis();

            if (now - started > interval) {
              sink.success(toResponse(now));
              return;
            }

            LockSupport.parkNanos(SLEEP_PERIOD_NS);
          }
        });
  }

  private String toResponse(long now) {
    String currentThread = Thread.currentThread().getName();
    final LocalDateTime time =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault());
    return "|" + currentThread + "| response: " + time;
  }
}
