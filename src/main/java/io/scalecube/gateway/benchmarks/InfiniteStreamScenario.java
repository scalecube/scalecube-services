package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.SERVICE_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class InfiniteStreamScenario {

  public static final String QUALIFIER = "/benchmarks/infiniteStream";

  private InfiniteStreamScenario() {
    // Do not instantiate
  }

  /**
   * Runner function for benchmarks.
   *
   * @param args program arguments
   * @param benchmarkStateFactory producer function for {@link AbstractBenchmarkState}
   */
  public static void runWith(
      String[] args, Function<BenchmarkSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    int numOfThreads = Runtime.getRuntime().availableProcessors() * 4;
    Duration rampUpDuration = Duration.ofSeconds(numOfThreads);

    BenchmarkSettings settings =
        BenchmarkSettings.from(args)
            .injectors(numOfThreads)
            .messageRate(1) // workaround
            .warmUpDuration(Duration.ofSeconds(30))
            .rampUpDuration(rampUpDuration)
            .executionTaskDuration(Duration.ofSeconds(900))
            .consoleReporterEnabled(true)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          BenchmarkTimer timer = state.timer("latency.timer");
          LatencyHelper latencyHelper = new LatencyHelper(state);

          ClientMessage request = ClientMessage.builder().qualifier(QUALIFIER).build();

          return client ->
              (executionTick, task) ->
                  client
                      .requestStream(request)
                      .doOnNext(
                          msg -> {
                            long timestamp = Long.parseLong(msg.header(SERVICE_RECV_TIME));
                            timer.update(
                                System.currentTimeMillis() - timestamp, TimeUnit.MILLISECONDS);
                            latencyHelper.calculate(msg);
                          });
        },
        (state, client) -> client.close());
  }
}
