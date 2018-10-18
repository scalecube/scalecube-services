package io.scalecube.services.benchmarks.service;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarkServiceImpl implements BenchmarkService {

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
  public Flux<ServiceMessage> infiniteStream(ServiceMessage message) {
    Callable<ServiceMessage> callable =
        () ->
            ServiceMessage.from(message)
                .header(SERVICE_SEND_TIME, System.currentTimeMillis())
                .build();
    return Mono.fromCallable(callable)
        .subscribeOn(Schedulers.parallel())
        .repeat()
        .onBackpressureDrop();
  }
}
