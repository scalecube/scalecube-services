package io.scalecube.gateway.benchmarks.example;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.stream.LongStream;

public class ExampleServiceImpl implements ExampleService {

  @Override
  public Mono<String> one(String name) {
    return Mono.just("Echo:" + name);
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return Flux.fromStream(LongStream.range(0, cnt).boxed())
        .publishOn(Schedulers.parallel());
  }

  @Override
  public Flux<Long> manyStreamWithBackpressureDrop(Long cnt) {
    return Flux.fromStream(LongStream.range(0, cnt).boxed())
        .publishOn(Schedulers.parallel(), Integer.MAX_VALUE)
        .onBackpressureDrop();
  }

}
