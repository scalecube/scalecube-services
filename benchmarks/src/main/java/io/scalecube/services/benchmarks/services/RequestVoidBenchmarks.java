package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;

public class RequestVoidBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl())
        .runForAsync(
            state -> {
              BenchmarkService benchmarkService = state.service(BenchmarkService.class);
              BenchmarksTimer timer = state.timer("timer");

              return i -> {
                Context timeContext = timer.time();
                return benchmarkService.oneWay("hello").doOnTerminate(timeContext::stop);
              };
            });
  }
}
