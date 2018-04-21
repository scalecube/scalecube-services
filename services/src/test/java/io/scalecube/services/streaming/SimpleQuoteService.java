package io.scalecube.services.streaming;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SimpleQuoteService implements QuoteService {

  final AtomicInteger i = new AtomicInteger(1);

  public SimpleQuoteService() {}

  @Override
  public Mono<String> justOne() {
    return Mono.just("1");
  }

  @Override
  public Flux<String> scheduled(int interval) {
    return Flux.interval(Duration.ofSeconds(1)).map(s -> "quote : " + i.incrementAndGet());
  }

  @Override
  public Flux<String> quotes(int maxSize) {
    return Flux.interval(Duration.ofSeconds(1)).map(s -> "quote : " + i.incrementAndGet());
  }

  @Override
  public Flux<String> snapshoot(int size) {
    return Flux.interval(Duration.ofSeconds(1)).map(s -> "quote : " + i.incrementAndGet());
  }
}
