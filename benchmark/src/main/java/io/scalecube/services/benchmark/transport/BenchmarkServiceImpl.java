package io.scalecube.services.benchmark.transport;

import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {
  @Override
  public Mono<Void> fireAndForget(SimpleBenchmarkRequest request) {
    return Mono.empty();
  }
}
