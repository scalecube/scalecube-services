package io.scalecube.services.benchmarks;

import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {

  @Override
  public Mono<Void> fireAndForget0() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> fireAndForget(BenchmarkMessage request) {
    return Mono.empty();
  }

  @Override
  public Mono<BenchmarkMessage> requestOne(BenchmarkMessage request) {
    return Mono.just(request);
  }
}
