package io.scalecube.services.examples;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import java.util.concurrent.Callable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    Builder builder = ServiceMessage.from(message);
    return Flux.range(0, Integer.MAX_VALUE)
        .map(i -> builder.header(SERVICE_SEND_TIME, System.currentTimeMillis()).build());
  }
}
