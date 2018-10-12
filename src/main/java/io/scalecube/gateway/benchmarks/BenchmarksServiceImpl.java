package io.scalecube.gateway.benchmarks;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarksServiceImpl implements BenchmarksService {

  @Override
  public Mono<ServiceMessage> one(ServiceMessage message) {
    Callable<ServiceMessage> callable =
        () -> {
          long value = System.currentTimeMillis();
          return ServiceMessage.from(message)
              .header(SERVICE_RECV_TIME, value)
              .header(SERVICE_SEND_TIME, value)
              .data("hello")
              .build();
        };
    return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel());
  }

  @Override
  public Mono<ServiceMessage> failure(ServiceMessage message) {
    return Mono.defer(() -> Mono.error(new RuntimeException("General failure")));
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
