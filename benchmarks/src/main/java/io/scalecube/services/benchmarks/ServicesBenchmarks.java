package io.scalecube.services.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(1)
@Threads(Threads.MAX)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class ServicesBenchmarks {

  private static final BenchmarkMessage MESSAGE = new BenchmarkMessage("benchmarkMessage");

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void fireAndForget0AverageTime(ServicesBenchmarksState state) {
    fireAndForget0(state);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void fireAndForget0Throughput(ServicesBenchmarksState state) {
    fireAndForget0(state);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void fireAndForgetAverageTime(ServicesBenchmarksState state) {
    fireAndForget(state);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void fireAndForgetThroughput(ServicesBenchmarksState state) {
    fireAndForget(state);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void requestOneAverageTime(ServicesBenchmarksState state) {
    requestOne(state);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void requestOneThroughput(ServicesBenchmarksState state) {
    requestOne(state);
  }

  private void fireAndForget0(ServicesBenchmarksState state) {
    state.service().fireAndForget0().block();
  }

  private void fireAndForget(ServicesBenchmarksState state) {
    state.service().fireAndForget(MESSAGE).block();
  }

  private void requestOne(ServicesBenchmarksState state) {
    state.service().requestOne(MESSAGE).block();
  }
}
