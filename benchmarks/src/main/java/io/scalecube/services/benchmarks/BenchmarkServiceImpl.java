package io.scalecube.services.benchmarks;

import java.util.stream.IntStream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {
  @Override
  public Mono<Void> fireAndForget0() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> voidRequestResponse(BenchmarkMessage request) {
    return Mono.empty();
  }

  @Override
  public void fireAndForget(BenchmarkMessage request) {}

  @Override
  public Mono<BenchmarkMessage> requestResponse(BenchmarkMessage request) {
    return Mono.just(request);
  }

  @Override
  public Flux<BenchmarkMessage> requestMany(BenchmarkMessage request) {
    int count = 10;
    // try {
    // count = Integer.valueOf(request.text);
    // } catch (Exception ignored) {
    // }
    return Flux.fromStream(IntStream.range(0, count).mapToObj(i -> request));
  }
}
