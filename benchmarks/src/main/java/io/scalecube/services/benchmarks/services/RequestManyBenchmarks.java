package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksMeter;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;

public class RequestManyBenchmarks {

  private static final String RESPONSE_COUNT = "1000";

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
              int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
              BenchmarksTimer timer = state.timer("timer");
              BenchmarksMeter meter = state.meter("responses");

              return i -> {
                Context timeContext = timer.time();
                return benchmarkService
                    .requestMany(responseCount)
                    .doOnNext(onNext -> meter.mark())
                    .doFinally(next -> timeContext.stop());
              };
            });
  }
}
