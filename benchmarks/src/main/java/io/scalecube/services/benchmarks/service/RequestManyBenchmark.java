package io.scalecube.services.benchmarks.service;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import java.util.concurrent.TimeUnit;

public class RequestManyBenchmark {

  private static final String RESPONSE_COUNT = "1000";

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    BenchmarkSettings settings = BenchmarkSettings.from(args).build();
    new BenchmarkServiceState(settings, new BenchmarkServiceImpl())
        .runForAsync(
            state -> {
              BenchmarkService benchmarkService = state.service(BenchmarkService.class);
              int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
              BenchmarkTimer timer = state.timer("timer");
              return i -> {
                Context timeContext = timer.time();
                return benchmarkService
                    .requestStreamRange(responseCount)
                    .doOnNext(
                        msg -> {
                          long time = Long.valueOf(msg.header("time"));
                          timer.update(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        })
                    .doFinally(next -> timeContext.stop());
              };
            });
  }
}
