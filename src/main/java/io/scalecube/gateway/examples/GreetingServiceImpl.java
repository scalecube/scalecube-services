package io.scalecube.gateway.examples;

import java.time.Duration;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class GreetingServiceImpl implements GreetingService {

  private Flux<Long> source = Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed()).share();

  @Override
  public Mono<String> one(String name) {
    return Mono.just("Echo:" + name);
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return Flux.fromStream(LongStream.range(0, cnt).boxed()).publishOn(Schedulers.parallel());
  }

  @Override
  public Mono<String> failingOne(String name) {
    return Mono.error(new RuntimeException(name));
  }

  @Override
  public Flux<String> many(String name) {
    return Flux.interval(Duration.ofMillis(100)).map(i -> "Greeting (" + i + ") to: " + name);
  }

  @Override
  public Flux<String> failingMany(String name) {
    return Flux.push(
        sink -> {
          sink.next("Echo:" + name);
          sink.next("Echo:" + name);
          sink.error(new RuntimeException("Echo:" + name));
        });
  }

  @Override
  public Mono<GreetingResponse> pojoOne(GreetingRequest request) {
    return one(request.getText()).map(GreetingResponse::new);
  }

  @Override
  public Flux<GreetingResponse> pojoMany(GreetingRequest request) {
    return many(request.getText()).map(GreetingResponse::new);
  }

  @Override
  public Mono<String> emptyOne(String name) {
    return Mono.empty();
  }

  @Override
  public Flux<String> emptyMany(String name) {
    return Flux.empty();
  }

  @Override
  public Mono<String> neverOne(String name) {
    return Mono.never();
  }

  @Override
  public Mono<String> delayOne(String name) {
    return Mono.delay(Duration.ofSeconds(1)).then(Mono.just(name));
  }

  @Override
  public Flux<String> delayMany(String name) {
    return Flux.interval(Duration.ofMillis(500), Duration.ofSeconds(2)).map(i -> name);
  }
}
