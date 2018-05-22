package io.scalecube.services.benchmark.transport;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(2)
@State(Scope.Benchmark)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HeadAndTailBenchmark {


  @Setup
  public void setup() {

    // Integer first = 1;
    // int size = 10;
    // List<Integer> source = IntStream.rangeClosed(first, size).boxed().collect(Collectors.toList());
    // Flux<Integer> requests = UnicastProcessor.fromStream(source.stream());
    //
    // Integer[] actual = Flux.from(HeadAndTail.createFrom(requests))
    // .flatMap(pair -> {
    // assertEquals(first, pair.head());
    // return Flux.from(pair.tail());
    // })
    // .toStream().toArray(Integer[]::new);


  }

  @Benchmark
  public void first() {}

}
