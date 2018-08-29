package io.scalecube.gateway.benchmarks;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.examples.GreetingService;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestMaxStreamBenchmark {

  public static final String QUALIFIER = "/" + GreetingService.QUALIFIER + "/rawStream";

  private RequestMaxStreamBenchmark() {
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

    int injectors = Runtime.getRuntime().availableProcessors() * 2;
    int messageRate = 1; // doesn't count in this test

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
            .injectors(injectors)
            .messageRate(messageRate)
            .rampUpDuration(Duration.ofSeconds(10))
            .executionTaskDuration(Duration.ofSeconds(300))
            .consoleReporterEnabled(true)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("service-stream-timer");
          return (executionTick, client) ->
              client
                  .requestStream(ClientMessage.builder().qualifier(QUALIFIER).build())
                  .doOnNext(
                      msg -> {
                        long serverTimestamp =
                            Long.parseLong(msg.headers().get(GreetingService.TIMESTAMP_KEY));
                        timer.update(
                            System.currentTimeMillis() - serverTimestamp, TimeUnit.MILLISECONDS);
                      });
        },
        (state, client) -> client.close());
  }
}
