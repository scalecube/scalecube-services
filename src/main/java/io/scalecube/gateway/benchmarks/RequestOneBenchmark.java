package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_RECV_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_SEND_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_CLIENT_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_SERVICE_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.SERVICE_RECV_TIME;

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
          BenchmarkTimer clientToGwTimer = state.timer("latency.client-to-gw-timer");
          BenchmarkTimer gwToServiceTimer = state.timer("latency.gw-to-service-timer");
          BenchmarkTimer serviceToGwTimer = state.timer("latency.service-to-gw-timer");
          BenchmarkTimer gwToClientTimer = state.timer("latency.gw-to-client-timer");

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
                                  timeContext.stop();
                                  calculateLatency(
                                      msg,
                                      clientToGwTimer,
                                      gwToServiceTimer,
                                      serviceToGwTimer,
                                      gwToClientTimer);
                                })
                            .doOnTerminate(task::scheduleNow);
                      });
        },
        (state, client) -> client.close());
  }

  private static void calculateLatency(
      ClientMessage message,
      BenchmarkTimer clientToGwTimer,
      BenchmarkTimer gwToServiceTimer,
      BenchmarkTimer serviceToGwTimer,
      BenchmarkTimer gwToClientTimer) {

    long clientSendTime = Long.parseLong(message.header(CLIENT_SEND_TIME));
    long clientRecvTime = Long.parseLong(message.header(CLIENT_RECV_TIME));
    long gwRecvFromClientTime = Long.parseLong(message.header(GW_RECV_FROM_CLIENT_TIME));
    long gwRecvFromServiceTime = Long.parseLong(message.header(GW_RECV_FROM_SERVICE_TIME));
    long serviceRecvTime = Long.parseLong(message.header(SERVICE_RECV_TIME));

    clientToGwTimer.update(gwRecvFromClientTime - clientSendTime, TimeUnit.MILLISECONDS);
    gwToClientTimer.update(clientRecvTime - gwRecvFromServiceTime, TimeUnit.MILLISECONDS);
    gwToServiceTimer.update(serviceRecvTime - gwRecvFromClientTime, TimeUnit.MILLISECONDS);
    serviceToGwTimer.update(gwRecvFromServiceTime - serviceRecvTime, TimeUnit.MILLISECONDS);
  }
}
