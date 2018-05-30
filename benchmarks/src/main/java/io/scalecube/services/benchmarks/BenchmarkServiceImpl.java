package io.scalecube.services.benchmarks;

import java.util.stream.IntStream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {

  @Override
  public Mono<Void> oneWay(BenchmarkMessage request) {
    return Mono.empty();
  }

  @Override
  public Mono<BenchmarkMessage> requestOne(BenchmarkMessage request) {
    return Mono.just(request);
  }

  @Override
  public Flux<BenchmarkMessage> requestMany(BenchmarkMessage request) {
    int count = 10;
    try {
      count = Integer.valueOf(request.text);
    } catch (Exception ignored) {
    }
    return Flux.fromStream(IntStream.range(0, count).mapToObj(i -> request));
  }
}
