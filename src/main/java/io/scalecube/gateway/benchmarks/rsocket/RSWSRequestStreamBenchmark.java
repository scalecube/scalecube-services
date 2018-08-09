package io.scalecube.gateway.benchmarks.rsocket;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.examples.GreetingService;

import com.codahale.metrics.Timer;
import io.scalecube.gateway.examples.StreamRequest;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RSWSRequestStreamBenchmark {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args)
        .injectors(900)
        .rampUpDuration(Duration.ofSeconds(30))
        .messageRate(9000)
        .executionTaskDuration(Duration.ofSeconds(60))
        .consoleReporterEnabled(true)
        .durationUnit(TimeUnit.MILLISECONDS)
        .build();

    new RSWSBenchmarkState(settings).runWithRampUp(
        (rampUpTick, state) -> state.createClient(),

        state -> {
          Timer timer = state.timer("notification.latency");
          StreamRequest streamRequest = new StreamRequest()
            .setIntervalMillis(settings.executionTaskInterval().toMillis())
            .setMessagesPerInterval(settings.messagesPerExecutionInterval());
          return (executionTick, client) -> {
            GreetingService service = client.forService(GreetingService.class);
            return service
                .requestInfiniteStream(streamRequest)
                .doOnNext(next -> timer.update(System.currentTimeMillis() - next, TimeUnit.MILLISECONDS));
          };
        },

        (state, client) -> client.close());
  }
}
