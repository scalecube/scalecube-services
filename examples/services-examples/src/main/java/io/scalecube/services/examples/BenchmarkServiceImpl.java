package io.scalecube.services.examples;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.Callable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class BenchmarkServiceImpl implements BenchmarkService {

  @Override
  public Mono<Void> requestVoid(ServiceMessage request) {
    return Mono.empty();
  }

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
    return Mono.fromCallable(callable);
  }

  @Override
  public Mono<ServiceMessage> failure(ServiceMessage message) {
    return Mono.defer(() -> Mono.error(new RuntimeException("General failure")));
  }

  @Override
  public Flux<ServiceMessage> infiniteStream(ServiceMessage message) {
    Callable<ServiceMessage> callable =
        () ->
            ServiceMessage.from(message)
                .header(SERVICE_SEND_TIME, System.currentTimeMillis())
                .build();
    return Mono.fromCallable(callable).subscribeOn(Schedulers.parallel()).repeat();
  }
}
