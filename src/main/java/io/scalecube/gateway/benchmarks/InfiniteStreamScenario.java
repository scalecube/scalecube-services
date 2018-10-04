package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientMessage.Builder;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfiniteStreamScenario {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfiniteStreamScenario.class);

  public static final String QUALIFIER = "/benchmarks/infiniteStream";

  private static final String RATE_LIMIT = "rateLimit";

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

    int numOfThreads = Runtime.getRuntime().availableProcessors();
    Duration rampUpDuration = Duration.ofSeconds(numOfThreads);

    BenchmarkSettings settings =
        BenchmarkSettings.from(args)
            .injectors(numOfThreads)
            .messageRate(1) // workaround
            .rampUpDuration(rampUpDuration)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          LatencyHelper latencyHelper = new LatencyHelper(state);

          Integer rateLimit = rateLimit(settings);

          Builder builder = ClientMessage.builder().qualifier(QUALIFIER);
          Optional.ofNullable(rateLimit).ifPresent(builder::rateLimit);
          ClientMessage request = builder.build();

          return client ->
              (executionTick, task) ->
                  client
                      .requestStream(request, task.scheduler())
                      .doOnError(th -> LOGGER.warn("Exception occured on requestStream: " + th))
                      .doOnNext(latencyHelper::calculate);
        },
        (state, client) -> client.close());
  }

  private static Integer rateLimit(BenchmarkSettings settings) {
    return Optional.ofNullable(settings.find(RATE_LIMIT, null)).map(Integer::parseInt).orElse(null);
  }
}
