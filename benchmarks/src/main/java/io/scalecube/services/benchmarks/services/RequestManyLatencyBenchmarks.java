package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import java.util.concurrent.TimeUnit;

public class RequestManyLatencyBenchmarks {

  private static final String RESPONSE_COUNT = "1000";

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
              int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
              BenchmarkTimer timer = state.timer("timer");
              return i -> {
                Context timeContext = timer.time();
                return benchmarkService
                    .nanoTime(responseCount)
                    .doOnNext(
                        onNext -> {
                          timer.update(System.nanoTime() - onNext, TimeUnit.NANOSECONDS);
                        })
                    .doFinally(next -> timeContext.stop());
              };
            });
  }
}
