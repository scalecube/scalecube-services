package io.scalecube.gateway.benchmarks;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestOneBenchmark {

  private RequestOneBenchmark() {
    // Do not instantiate
  }

  public static void runWith(
      String[] args,
      Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
            .injectors(1000)
            .messageRate(100_000)
            .rampUpDuration(Duration.ofSeconds(60))
            .executionTaskDuration(Duration.ofSeconds(300))
            .consoleReporterEnabled(true)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("service-one-timer");
          return (executionTick, client) -> {
            Timer.Context timeContext = timer.time();
            ExampleService service = client.forService(ExampleService.class);
            return service.one("hello").doOnTerminate(timeContext::stop);
          };
        },
        (state, client) -> client.close());
  }
}
