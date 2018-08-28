package io.scalecube.gateway.benchmarks;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestBroadcastStreamBenchmark {

  private RequestBroadcastStreamBenchmark() {
    // Do not instantiate
  }

  public static void runWith(
      String[] args,
      Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
            .injectors(1000)
            .messageRate(1) // workaround
            .rampUpDuration(Duration.ofSeconds(60))
            .executionTaskDuration(Duration.ofSeconds(300))
            .consoleReporterEnabled(true)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("service-stream-timer");

          return (executionTick, client) -> {
            ExampleService service = client.forService(ExampleService.class);
            return service
                .broadcastStream()
                .doOnNext(
                    next -> timer.update(System.currentTimeMillis() - next, TimeUnit.MILLISECONDS));
          };
        },
        (state, client) -> client.close());
  }
}
