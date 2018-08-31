package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestOneBenchmark {

  public static final String QUALIFIER = "/benchmarks/one";

  private RequestOneBenchmark() {
    // Do not instantiate
  }

  /**
   * Runner function for benchmarks.
   *
   * @param args program arguments
   * @param benchmarkStateFactory producer function for {@link AbstractBenchmarkState}
   */
  public static void runWith(
      String[] args,
      Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
            .injectors(Runtime.getRuntime().availableProcessors())
            .messageRate((int) 100e3)
            .warmUpDuration(Duration.ofSeconds(30))
            .rampUpDuration(Duration.ofSeconds(10))
            .executionTaskDuration(Duration.ofSeconds(900))
            .consoleReporterEnabled(true)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          BenchmarksTimer timer = state.timer("timer-total");

          return (executionTick, client) -> {
            Context timeContext = timer.time();
            ClientMessage request = ClientMessage.builder().qualifier(QUALIFIER).build();
            return client.requestResponse(request).doOnTerminate(timeContext::stop);
          };
        },
        (state, client) -> client.close());
  }
}
