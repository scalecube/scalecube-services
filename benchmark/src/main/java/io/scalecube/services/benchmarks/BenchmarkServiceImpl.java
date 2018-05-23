package io.scalecube.services.benchmarks;

import org.openjdk.jmh.infra.Blackhole;

import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {

  private final Blackhole bh;

  public BenchmarkServiceImpl(Blackhole bh) {
    this.bh = bh;
  }

  @Override
  public Mono<Void> fireAndForget0() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> fireAndForget(BenchmarkMessage request) {
    if (bh != null) {
      bh.consume(request);
    }
    return Mono.empty();
  }

  @Override
  public Mono<BenchmarkMessage> requestOne(BenchmarkMessage request) {
    if (bh != null) {
      bh.consume(request);
    }
    return Mono.just(request);
  }
}
