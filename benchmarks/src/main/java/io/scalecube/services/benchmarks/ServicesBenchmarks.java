package io.scalecube.services.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@Threads(4)
@Warmup(iterations = 2)
@Measurement(iterations = 2, time = 5)
public class ServicesBenchmarks {

  private static final BenchmarkMessage MESSAGE = new BenchmarkMessage("benchmarkMessage");

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void fireAndForgetAverageTime(ServicesBenchmarksState state, Blackhole bh) {
    fireAndForget(state, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void fireAndForgetThroughput(ServicesBenchmarksState state, Blackhole bh) {
    fireAndForget(state, bh);
  }

  private void fireAndForget(ServicesBenchmarksState state, Blackhole bh) {
    state.service().fireAndForget(MESSAGE).subscribe(bh::consume);
  }
}
