package io.scalecube.services.benchmarks;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.HeadAndTail;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Fork(1)
@Threads(Threads.MAX)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
public class HeadAndTailBenchmarks {

  private static final ServiceMessage MESSAGE = ServiceMessage.builder()
      .qualifier("benchmark/test")
      .data("{\"greeting\":\"hello\"}")
      .dataFormat("application/json")
      .header("key1", "value1")
      .header("key2", "value2")
      .build();

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void headAndTailAverageTime() {
    headAndTail();
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void headAndTailThroughput() {
    headAndTail();
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void flatMapAverageTime() {
    flatMap();
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void flatMapThroughput() {
    flatMap();
  }

  private void flatMap() {
    Flux.just(MESSAGE).flatMap(Flux::just).blockLast();
  }

  private void headAndTail() {
    Flux.from(HeadAndTail.createFrom(Mono.just(MESSAGE)))
        .flatMap(pair -> Flux.from(pair.tail()).startWith(pair.head()))
        .blockLast();
  }
}
