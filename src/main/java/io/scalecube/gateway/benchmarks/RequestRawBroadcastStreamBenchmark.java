package io.scalecube.gateway.benchmarks;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.services.api.Qualifier;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestRawBroadcastStreamBenchmark {

  private static final String QUALIFIER =
      Qualifier.asString(GreetingService.QUALIFIER, "rawBroadcastStream");

  private RequestRawBroadcastStreamBenchmark() {
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
          return (executionTick, client) ->
              client
                  .rawStream(ClientMessage.builder().qualifier(QUALIFIER).build())
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
