package io.scalecube.services.benchmarks.services;

import java.util.stream.IntStream;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BenchmarkServiceImpl implements BenchmarkService {

  @Override
  public Mono<Void> oneWay(String request) {
    return Mono.empty();
  }

  @Override
  public Mono<String> requestOne(String request) {
    return Mono.just(request);
  }

  @Override
  public Flux<String> requestMany(int count) {
    return Flux.fromStream(IntStream.range(0, count).mapToObj(i -> "response-" + i));
  }

  @Override
  public Flux<String> requestManyNoParams() {
    return Flux.fromStream(IntStream.range(0, (int) 1e3).mapToObj(i -> "response-" + i));
  }

  @Override
  public Flux<Long> nanoTime(int count) {
    return Flux.fromStream(IntStream.range(0, count).mapToObj(i -> System.nanoTime()));
  }

  @Override
  public Flux<String> requestBidirectionalEcho(Flux<String> counts) {
    return EmitterProcessor.create(
        emitter -> counts.subscribe(emitter::next, emitter::error, emitter::complete));
  }
}
