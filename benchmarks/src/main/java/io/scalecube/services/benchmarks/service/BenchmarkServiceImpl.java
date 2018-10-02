package io.scalecube.services.benchmarks.service;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarkServiceImpl implements BenchmarkService {

  @Override
  public Mono<Void> oneWay(ServiceMessage request) {
    return Mono.<Void>empty().subscribeOn(Schedulers.parallel());
  }

  @Override
  public Mono<ServiceMessage> requestOne(ServiceMessage request) {
    return Mono.just(request).subscribeOn(Schedulers.parallel());
  }

  @Override
  public Flux<ServiceMessage> requestStreamRange(int count) {
    return Flux.defer(
        () -> {
          Callable<ServiceMessage> callable =
              () ->
                  ServiceMessage.builder()
                      .qualifier("/benchmarks/requestStreamRange")
                      .header("time", String.valueOf(System.nanoTime()))
                      .build();
          return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel()).repeat(count);
        });
  }
}
