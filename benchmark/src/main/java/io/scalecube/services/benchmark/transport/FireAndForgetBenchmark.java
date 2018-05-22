package io.scalecube.services.benchmark.transport;

import io.scalecube.services.Microservices;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@State(Scope.Benchmark)
@Threads(4)
@Warmup(iterations = 2)
@Measurement(iterations = 2, time = 5)
public class FireAndForgetBenchmark {

  private static final SimpleBenchmarkRequest REQUEST = new SimpleBenchmarkRequest("monoVoidServiceBenchmark");

  private Microservices seed;
  private Microservices node;
  private BenchmarkService benchmarkService;

  @Setup(Level.Trial)
  public void setup() {
    seed = Microservices.builder().build().startAwait();
    node = Microservices.builder()
        .seeds(seed.cluster().address())
        .services(new BenchmarkServiceImpl())
        .build()
        .startAwait();

    benchmarkService = seed.call().create().api(BenchmarkService.class);
  }

  @TearDown
  public void tearDown() {
    if (node != null) {
      node.shutdown().block();
      node = null;
    }
    if (seed != null) {
      seed.shutdown().block();
      seed = null;
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void avgtOfFireAndForget(Blackhole bh) {
    fireAndForget(bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void thrptOfFireAndForget(Blackhole bh) {
    fireAndForget(bh);
  }

  private void fireAndForget(Blackhole bh) {
    benchmarkService.fireAndForget(REQUEST)
        .doOnSuccess(bh::consume).block();
  }
}
