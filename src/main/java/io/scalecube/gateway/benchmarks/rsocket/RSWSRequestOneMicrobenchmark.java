package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;

import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RSWSRequestOneMicrobenchmark {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args)
        .injectors(1000)
        .messageRate(100_000)
        .rampUpDuration(Duration.ofSeconds(60))
        .executionTaskDuration(Duration.ofSeconds(300))
        .consoleReporterEnabled(true)
        .durationUnit(TimeUnit.MILLISECONDS)
        .build();

    new RSWSMicrobenchmarkState(settings).runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("timer");
          return (executionTick, client) -> {
            Timer.Context timeContext = timer.time();
            ExampleService service = client.forService(ExampleService.class);
            return service.one("hello").doOnTerminate(timeContext::stop);
          };
        },
        (state, client) -> client.close());
  }
}
