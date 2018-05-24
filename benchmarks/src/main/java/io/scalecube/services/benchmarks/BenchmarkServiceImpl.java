package io.scalecube.services.benchmarks;

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
}
