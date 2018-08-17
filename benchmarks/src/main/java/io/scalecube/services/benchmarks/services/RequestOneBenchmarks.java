package io.scalecube.services.benchmarks.services;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;

public class RequestOneBenchmarks {

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
              Timer timer = state.timer("timer");

              return i -> {
                Timer.Context timeContext = timer.time();
                return benchmarkService.requestOne("hello").doOnTerminate(timeContext::stop);
              };
            });
  }
}
