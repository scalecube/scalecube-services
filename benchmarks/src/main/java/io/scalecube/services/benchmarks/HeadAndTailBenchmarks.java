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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Fork(2)
@State(Scope.Benchmark)
@Threads(4)
@Warmup(iterations = 2)
@Measurement(iterations = 2, time = 5)
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
  public void headAndTailAverageTime(Blackhole bh) {
    headAndTail(bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void headAndTailThroughput(Blackhole bh) {
    headAndTail(bh);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void flatMapAverageTime(Blackhole bh) {
    flatMap(bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void flatMapThroughput(Blackhole bh) {
    flatMap(bh);
  }

  private void flatMap(Blackhole bh) {
    Flux.just(MESSAGE).flatMap(Flux::just).subscribe(bh::consume);
  }

  private void headAndTail(Blackhole bh) {
    Flux.from(HeadAndTail.createFrom(Mono.just(MESSAGE)))
        .flatMap(pair -> Flux.from(pair.tail()).startWith(pair.head()))
        .subscribe(bh::consume);
  }
}
