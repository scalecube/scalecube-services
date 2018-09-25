package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class RequestOneScenario {

  private static final String QUALIFIER = "/benchmarks/one";

  private RequestOneScenario() {
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

    int numOfThreads = Runtime.getRuntime().availableProcessors() * 2;
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
                  Mono.defer(
                      () -> {
                        Context timeContext = timer.time();
                        return client
                            .requestResponse(request, Schedulers.parallel())
                            .doOnNext(
                                msg -> {
                                  Optional.ofNullable(msg.data())
                                      .ifPresent(ReferenceCountUtil::safestRelease);
                                  latencyHelper.calculate(msg);
                                })
                            .doOnTerminate(
                                () -> {
                                  timeContext.stop();
                                  task.scheduler().schedule(task);
                                });
                      });
        },
        (state, client) -> client.close());
  }
}
