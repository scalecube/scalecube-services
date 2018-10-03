package io.scalecube.services.benchmarks.service;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;

public class RequestVoidBenchmark {

  private static final String QUALIFIER = "/benchmarks/requestVoid";

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    BenchmarkSettings settings =
        BenchmarkSettings.from(args)
            .warmUpDuration(Duration.ofSeconds(30))
            .executionTaskDuration(Duration.ofSeconds(300))
            .consoleReporterEnabled(true)
            .build();

    new BenchmarkServiceState(settings, new BenchmarkServiceImpl())
        .runForAsync(
            state -> {
              BenchmarkService benchmarkService = state.service(BenchmarkService.class);
              BenchmarkTimer timer = state.timer("latency.timer");
              ServiceMessage message = ServiceMessage.builder().qualifier(QUALIFIER).build();

              return i -> {
                Context timeContext = timer.time();
                return benchmarkService.requestVoid(message).doOnTerminate(timeContext::stop);
              };
            });
  }
}
