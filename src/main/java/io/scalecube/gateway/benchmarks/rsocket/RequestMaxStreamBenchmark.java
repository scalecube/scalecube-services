package io.scalecube.gateway.benchmarks.rsocket;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.AbstractBenchmarkState;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.StreamRequest;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RequestMaxStreamBenchmark {

  public static final String QUALIFIER = "/" + GreetingService.QUALIFIER + "/rawStream";

  private RequestMaxStreamBenchmark() {
    // Do not instantiate
  }

  public static void runWith(String[] args,
    Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    int injectors = Runtime.getRuntime().availableProcessors();
    int messageRate = 100_000;

    BenchmarksSettings settings = BenchmarksSettings.from(args)
      .injectors(injectors)
      .messageRate(100_000)
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
        StreamRequest streamRequest = new StreamRequest()
          .setIntervalMillis(settings.executionTaskInterval().toMillis())
          .setMessagesPerInterval(settings.messagesPerExecutionInterval());
        return (executionTick, client) -> client
          .rawStream(ClientMessage.builder().qualifier(QUALIFIER).build())
          .doOnNext(msg -> {
            long serverTimestamp =
              Long.parseLong(msg.headers().get(GreetingService.TIMESTAMP_KEY));
            timer.update(System.currentTimeMillis() - serverTimestamp, TimeUnit.MILLISECONDS);
          });
      },
      (state, client) -> client.close());
  }
}
