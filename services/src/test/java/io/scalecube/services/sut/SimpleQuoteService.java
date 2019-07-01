package io.scalecube.services.sut;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SimpleQuoteService implements QuoteService {

  final AtomicInteger counter = new AtomicInteger(1);

  public SimpleQuoteService() {}

  @Override
  public Mono<String> justOne() {
    return Mono.just("1");
  }

  @Override
  public Flux<String> scheduled(int interval) {
    return Flux.interval(Duration.ofSeconds(1)).map(s -> "quote : " + counter.incrementAndGet());
  }

  @Override
  public Flux<String> quotes() {
    return Flux.interval(Duration.ofSeconds(1)).map(s -> "quote : " + counter.incrementAndGet());
  }

  @Override
  public Flux<String> snapshot(int size) {
    return Flux.fromStream(IntStream.range(0, size).boxed().map(i -> "tick:" + i));
  }

  @Override
  public Mono<String> justNever() {
    return Mono.never();
  }

  @Override
  public Flux<String> justManyNever() {
    return Flux.never();
  }

  @Override
  public Flux<String> onlyOneAndThenNever() {
    return Flux.merge(Mono.just("only first"), Mono.never());
  }
}
