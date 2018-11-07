package io.scalecube.services.benchmarks.gateway;

import static io.scalecube.services.examples.BenchmarkService.CLIENT_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.services.benchmarks.LatencyHelper;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.concurrent.Queues;

public final class InfiniteStreamScenario {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfiniteStreamScenario.class);

  public static final String QUALIFIER = "/benchmarks/infiniteStream";

  private static final int DEFAULT_RATE_LIMIT = Queues.SMALL_BUFFER_SIZE;

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

          BenchmarkMeter serviceToClientMeter = state.meter("meter.service-to-client");

          Integer rateLimit = rateLimit(settings);

          ClientMessage request =
              ClientMessage.builder().qualifier(QUALIFIER).rateLimit(rateLimit).build();

          return client ->
              (executionTick, task) ->
                  Flux.defer(
                      () ->
                          client
                              .requestStream(request)
                              .map(InfiniteStreamScenario::enichResponse)
                              .limitRate(rateLimit)
                              .doOnNext(
                                  message -> {
                                    serviceToClientMeter.mark();
                                    latencyHelper.calculate(message);
                                  })
                              .doOnError(
                                  th -> LOGGER.warn("Exception occured on requestStream: " + th)));
        },
        (state, client) -> client.close());
  }

  private static Integer rateLimit(BenchmarkSettings settings) {
    return Optional.ofNullable(settings.find(RATE_LIMIT, null))
        .map(Integer::parseInt)
        .orElse(DEFAULT_RATE_LIMIT);
  }

  private static ClientMessage enichResponse(ClientMessage msg) {
    return ClientMessage.from(msg).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }
}
