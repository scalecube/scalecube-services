package io.scalecube.gateway.benchmarks;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarksServiceImpl implements BenchmarksService {

  private Flux<Long> sharedSource =
      Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed()).share();

  @Override
  public Mono<ServiceMessage> one(ServiceMessage message) {
    return Mono.defer(
        () -> {
          String value = String.valueOf(System.currentTimeMillis());
          return Mono.just(
              ServiceMessage.from(message)
                  .header(SERVICE_RECV_TIME, value)
                  .header(SERVICE_SEND_TIME, value)
                  .data("hello")
                  .build());
        });
  }

  @Override
  public Mono<ServiceMessage> failure(ServiceMessage message) {
    return Mono.defer(() -> Mono.error(new RuntimeException("General failure")));
  }

  @Override
  public Flux<ServiceMessage> broadcastStream(ServiceMessage message) {
    return Flux.defer(
        () ->
            sharedSource
                .subscribeOn(Schedulers.parallel())
                .map(
                    i ->
                        ServiceMessage.builder()
                            .qualifier(message.qualifier())
                            .header(SERVICE_SEND_TIME, String.valueOf(System.currentTimeMillis()))
                            .build()));
  }

  @Override
  public Flux<ServiceMessage> infiniteStream(ServiceMessage message) {
    return Flux.defer(
        () -> {
          Callable<ServiceMessage> callable =
              () ->
                  ServiceMessage.builder()
                      .qualifier(message.qualifier())
                      .header(SERVICE_SEND_TIME, Long.toString(System.currentTimeMillis()))
                      .build();
          return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel()).repeat();
        });
  }
}
