package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.INTERVAL_MILLIS;
import static io.scalecube.gateway.benchmarks.BenchmarksService.MESSAGES_PER_INTERVAL;
import static io.scalecube.gateway.benchmarks.BenchmarksService.TIMESTAMP_KEY;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class InfiniteStreamWithRateBenchmark {

  public static final String QUALIFIER = "/benchmarks/infiniteStreamWithRate";

  private InfiniteStreamWithRateBenchmark() {
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

          long interval = settings.executionTaskInterval().toMillis();
          int messagesPerInterval = settings.messagesPerExecutionInterval();
          ClientMessage request =
              ClientMessage.builder()
                  .qualifier(QUALIFIER)
                  .header(INTERVAL_MILLIS, Long.toString(interval))
                  .header(MESSAGES_PER_INTERVAL, Long.toString(messagesPerInterval))
                  .build();

          return (executionTick, client) ->
              client
                  .requestStream(request)
                  .doOnNext(
                      message -> {
                        long timestamp = Long.valueOf(message.headers().get(TIMESTAMP_KEY));
                        long total = System.currentTimeMillis() - timestamp;
                        timer.update(total, TimeUnit.MILLISECONDS);
                      });
        },
        (state, client) -> client.close());
  }
}
