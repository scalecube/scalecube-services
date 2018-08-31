package io.scalecube.gateway.benchmarks;

import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarksServiceImpl implements BenchmarksService {

  private Flux<Long> source = Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed()).share();

  @Override
  public Mono<ServiceMessage> one(ServiceMessage message) {
    return Mono.defer(() -> Mono.just(ServiceMessage.from(message).build()));
  }

  @Override
  public Mono<ServiceMessage> failure(ServiceMessage message) {
    return Mono.defer(() -> Mono.error(new RuntimeException("General failure")));
  }

  @Override
  public Flux<ServiceMessage> broadcastStream() {
    return Flux.defer(
        () ->
            source
                .subscribeOn(Schedulers.parallel())
                .map(
                    i ->
                        ServiceMessage.builder()
                            .header(TIMESTAMP_KEY, Long.toString(System.currentTimeMillis()))
                            .build()));
  }

  @Override
  public Flux<ServiceMessage> infiniteStream() {
    return Flux.defer(
        () ->
            Mono.fromCallable(
                    () ->
                        ServiceMessage.builder()
                            .header(TIMESTAMP_KEY, Long.toString(System.currentTimeMillis()))
                            .build())
                .subscribeOn(Schedulers.parallel())
                .repeat());
  }

  @Override
  public Flux<ServiceMessage> infiniteStreamWithRate(ServiceMessage message) {
    return Flux.defer(
        () -> {
          Duration intervalMillis =
              Duration.ofMillis(Long.valueOf(message.header(INTERVAL_MILLIS)));
          long messageNum = Long.valueOf(message.header(MESSAGES_PER_INTERVAL));

          Flux<ServiceMessage> repeat =
              Mono.fromCallable(
                      () ->
                          ServiceMessage.builder()
                              .header(TIMESTAMP_KEY, Long.toString(System.currentTimeMillis()))
                              .build())
                  .repeat(messageNum);

          return Flux.concat(Flux.interval(intervalMillis).map(tick -> repeat))
              .publishOn(Schedulers.parallel())
              .onBackpressureDrop();
        });
  }
}
