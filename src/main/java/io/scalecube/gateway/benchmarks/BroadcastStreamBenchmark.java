package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.TIMESTAMP_KEY;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class BroadcastStreamBenchmark {

  public static final String QUALIFIER = "/benchmarks/broadcastStream";

  private BroadcastStreamBenchmark() {
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
            .messageRate(1) // workaround
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

          ClientMessage request = ClientMessage.builder().qualifier(QUALIFIER).build();

          return (executionTick, client) ->
              client
                  .requestStream(request)
                  .doOnNext(
                      message -> {
                        long timestamp = Long.parseLong(message.headers().get(TIMESTAMP_KEY));
                        long total = System.currentTimeMillis() - timestamp;
                        timer.update(total, TimeUnit.MILLISECONDS);
                      });
        },
        (state, client) -> client.close());
  }
}
