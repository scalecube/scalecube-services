package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import io.scalecube.gateway.examples.StreamRequest;

import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestStreamBenchmark {

  private RequestStreamBenchmark() {
    // Do not instantiate
  }

  public static void runWith(String[] args,
      Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    BenchmarksSettings settings = BenchmarksSettings.from(args)
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
          Timer timer = state.timer("service-stream-timer");
          StreamRequest streamRequest = new StreamRequest()
              .setIntervalMillis(settings.executionTaskInterval().toMillis())
              .setMessagesPerInterval(settings.messagesPerExecutionInterval());
          return (executionTick, client) -> {
            ExampleService service = client.forService(ExampleService.class);
            return service
                .requestInfiniteStream(streamRequest)
                .doOnNext(next -> timer.update(System.currentTimeMillis() - next, TimeUnit.MILLISECONDS));
          };
        },
        (state, client) -> client.close());
  }
}
