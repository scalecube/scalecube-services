package io.scalecube.services.benchmarks.service;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarkServiceImpl implements BenchmarkService {

  private Flux<Long> sharedSource =
      Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed()).share();

  @Override
  public Mono<Void> requestVoid(ServiceMessage request) {
    return Mono.<Void>empty().subscribeOn(Schedulers.parallel());
  }

  @Override
  public Mono<ServiceMessage> requestOne(ServiceMessage message) {
    Callable<ServiceMessage> callable =
        () -> {
          long value = System.currentTimeMillis();
          return ServiceMessage.from(message)
              .header(SERVICE_RECV_TIME, value)
              .header(SERVICE_SEND_TIME, value)
              .build();
        };
    return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel());
  }

  @Override
  public Flux<ServiceMessage> sharedStream(ServiceMessage message) {
    return Flux.defer(
        () ->
            sharedSource
                .subscribeOn(Schedulers.parallel())
                .map(
                    i ->
                        ServiceMessage.from(message)
                            .header(SERVICE_SEND_TIME, System.currentTimeMillis())
                            .build()));
  }

  @Override
  public Flux<ServiceMessage> infiniteStream(ServiceMessage message) {
    return Flux.defer(
        () -> {
          Callable<ServiceMessage> callable =
              () ->
                  ServiceMessage.from(message)
                      .header(SERVICE_SEND_TIME, System.currentTimeMillis())
                      .build();
          return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel()).repeat();
        });
  }
}
