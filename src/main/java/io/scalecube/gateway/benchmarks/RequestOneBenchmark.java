package io.scalecube.gateway.benchmarks;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public final class RequestOneBenchmark {

  private static final String QUALIFIER = "/benchmarks/one";

  private RequestOneBenchmark() {
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
          GatewayLatencyHelper latencyHelper = new GatewayLatencyHelper(state);

          ClientMessage request = ClientMessage.builder().qualifier(QUALIFIER).build();

          return client ->
              (executionTick, task) ->
                  Mono.defer(
                      () -> {
                        Context timeContext = timer.time();

                        return client
                            .requestResponse(request)
                            .doOnNext(
                                msg -> {
                                  if (msg.hasData(ByteBuf.class)) {
                                    ReferenceCountUtil.safeRelease(msg.data());
                                  }
                                  timeContext.stop();
                                  latencyHelper.calculate(msg);
                                })
                            .doOnTerminate(task::scheduleNow);
                      });
        },
        (state, client) -> client.close());
  }
}
