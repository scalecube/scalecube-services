package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import io.scalecube.gateway.examples.StreamRequest;

import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RSWSRequestStreamMicrobenchmark {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args)
        .injectors(1000)
        .rampUpDuration(Duration.ofSeconds(60))
        .messageRate(100_000)
        .executionTaskDuration(Duration.ofSeconds(300))
        .consoleReporterEnabled(true)
        .durationUnit(TimeUnit.MILLISECONDS)
        .build();

    new RSWSMicrobenchmarkState(settings).runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("notification.latency");
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
