package io.scalecube.services.benchmarks.service;

import io.scalecube.services.api.ServiceMessage;
import java.util.stream.IntStream;
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
    return Flux.fromStream(
            IntStream.range(0, count)
                .mapToObj(
                    i ->
                        ServiceMessage.builder()
                            .qualifier("/benchmarks/requestStreamRange")
                            .header("time", String.valueOf(System.nanoTime()))
                            .build()))
        .subscribeOn(Schedulers.parallel());
  }
}
