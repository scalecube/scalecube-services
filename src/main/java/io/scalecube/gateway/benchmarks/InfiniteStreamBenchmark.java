package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_RECV_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_SERVICE_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.SERVICE_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class InfiniteStreamBenchmark {

  public static final String QUALIFIER = "/benchmarks/infiniteStream";

  private InfiniteStreamBenchmark() {
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
          BenchmarkTimer serviceToGwTimer = state.timer("latency.service-to-gw-timer");
          BenchmarkTimer gwToClientTimer = state.timer("latency.gw-to-client-timer");

          ClientMessage request = ClientMessage.builder().qualifier(QUALIFIER).build();

          return client ->
              (executionTick, task) ->
                  client
                      .requestStream(request)
                      .doOnNext(
                          msg -> {
                            long timestamp = Long.parseLong(msg.header(SERVICE_RECV_TIME));
                            timer.update(
                                System.currentTimeMillis() - timestamp, TimeUnit.MILLISECONDS);
                            calculateReturnLatency(msg, serviceToGwTimer, gwToClientTimer);
                          });
        },
        (state, client) -> client.close());
  }

  private static void calculateReturnLatency(
      ClientMessage message, BenchmarkTimer serviceToGwTimer, BenchmarkTimer gwToClientTimer) {

    long clientRecvTime = Long.parseLong(message.header(CLIENT_RECV_TIME));
    long gwRecvFromServiceTime = Long.parseLong(message.header(GW_RECV_FROM_SERVICE_TIME));
    long serviceRecvTime = Long.parseLong(message.header(SERVICE_RECV_TIME));

    serviceToGwTimer.update(gwRecvFromServiceTime - serviceRecvTime, TimeUnit.MILLISECONDS);
    gwToClientTimer.update(clientRecvTime - gwRecvFromServiceTime, TimeUnit.MILLISECONDS);
  }
}
