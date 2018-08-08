package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;

import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RSWSRequestOneBenchmark {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args)
        .injectors(1000)
        .messageRate(10_000)
        .rampUpDuration(Duration.ofSeconds(60))
        .executionTaskDuration(Duration.ofSeconds(120))
        .consoleReporterEnabled(true)
        .durationUnit(TimeUnit.MILLISECONDS)
        .build();

    new RSWSBenchmarkState(settings).runWithRampUp(

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
