package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;

public class RequestVoidBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    BenchmarkSettings settings = BenchmarkSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl())
        .runForAsync(
            state -> {
              BenchmarkService benchmarkService = state.service(BenchmarkService.class);
              BenchmarkTimer timer = state.timer("timer");

              return i -> {
                Context timeContext = timer.time();
                return benchmarkService.oneWay("hello").doOnTerminate(timeContext::stop);
              };
            });
  }
}
