package io.scalecube.services.examples.services;

import io.scalecube.services.annotations.Inject;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.locks.LockSupport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

public class Service1Impl implements Service1 {

  private static final long SLEEP_PERIOD_NS = 10000;

  @Inject private Service2 remoteService;

  @Override
  public Flux<String> manyDelay(long interval) {
    return Flux.<String>create(sink -> sink.onRequest(r -> onRequest(sink, interval)))
        .subscribeOn(Schedulers.parallel())
        .log("manyDelay   |");
  }

  @Override
  public Flux<String> remoteCallThenManyDelay(long interval) {
    return remoteService
        .oneDelay(interval)
        .publishOn(Schedulers.parallel())
        .log("remoteCall  |")
        .then(
            remoteService.oneDelay(interval).publishOn(Schedulers.parallel()).log("remoteCall2  |"))
        .flatMapMany(
            i ->
                Flux.<String>create(sink -> sink.onRequest(r -> onRequest(sink, interval)))
                    .subscribeOn(Schedulers.parallel())
                    .log("manyInner   |"))
        .log("rcManyDelay |");
  }

  private void onRequest(FluxSink<String> sink, long interval) {
    long lastPublished = System.currentTimeMillis();

    while (!sink.isCancelled() && sink.requestedFromDownstream() > 0) {
      long now = System.currentTimeMillis();

      if (sink.requestedFromDownstream() > 0 && now - lastPublished > interval) {
        lastPublished = now;
        sink.next(toResponse(now));
        continue;
      }

      LockSupport.parkNanos(SLEEP_PERIOD_NS);
    }
  }

  private String toResponse(long now) {
    String currentThread = Thread.currentThread().getName();
    final LocalDateTime time =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault());
    return "|" + currentThread + "| response: " + time;
  }
}
