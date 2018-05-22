package io.scalecube.services.benchmark.transport;

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
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HeadAndTailBenchmark {

  private final ServiceMessage message = ServiceMessage.builder()
      .qualifier("benchmark/test")
      .data("{\"greeting\":\"hello\"}")
      .dataFormat("application/json")
      .header("key1", "value1")
      .header("key2", "value2")
      .build();

  @Benchmark
  public void headAndTail(Blackhole bh) {
    Flux.from(HeadAndTail.createFrom(Mono.just(message)))
        .flatMap(pair -> Flux.from(pair.tail()).startWith(pair.head()))
        .subscribe(bh::consume);
  }

  @Benchmark
  public void flatMap(Blackhole bh) {
    Flux.just(message).flatMap(Flux::just).subscribe(bh::consume);
  }
}
