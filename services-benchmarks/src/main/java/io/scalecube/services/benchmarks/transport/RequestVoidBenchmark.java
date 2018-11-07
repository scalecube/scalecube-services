package io.scalecube.services.benchmarks.transport;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.examples.BenchmarkService;
import io.scalecube.services.examples.BenchmarkServiceImpl;

public class RequestVoidBenchmark {

  private static final String QUALIFIER = "/benchmarks/requestVoid";

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
              BenchmarkMeter meter = state.meter("meter");
              ServiceMessage message = ServiceMessage.builder().qualifier(QUALIFIER).build();

              return i -> {
                Context timeContext = timer.time();
                return benchmarkService
                    .requestVoid(message)
                    .doOnTerminate(timeContext::stop)
                    .doOnTerminate(meter::mark);
              };
            });
  }
}
