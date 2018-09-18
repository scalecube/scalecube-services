package io.scalecube.services.benchmarks.service;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.api.ServiceMessage;

public class RequestVoidBenchmark {

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
              BenchmarkTimer timer = state.timer("timer");
              ServiceMessage message =
                  ServiceMessage.builder().qualifier("/benchmarks/oneWay").build();
              return i -> {
                Context timeContext = timer.time();
                return benchmarkService.oneWay(message).doOnTerminate(timeContext::stop);
              };
            });
  }
}
