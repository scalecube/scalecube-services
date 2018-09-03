package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_RECV_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_SEND_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_CLIENT_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_SERVICE_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.SERVICE_RECV_TIME;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
      String[] args,
      Function<BenchmarksSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    int numOfThreads = Runtime.getRuntime().availableProcessors();
    Duration rampUpDuration = Duration.ofSeconds(numOfThreads);

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
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
          BenchmarksTimer timer = state.timer("latency.timer");
          BenchmarksTimer clientToGatewayTimer = state.timer("latency.client-to-gw-timer");
          BenchmarksTimer gatewayToServiceTimer = state.timer("latency.gw-to-service-timer");
          BenchmarksTimer serviceToGatewayTimer = state.timer("latency.service-to-gw-timer");
          BenchmarksTimer gatewayToClientTimer = state.timer("latency.gw-to-client-timer");

          return (executionTick, client) -> {
            Context timeContext = timer.time();
            return client
                .requestResponse(ClientMessage.builder().qualifier(QUALIFIER).build())
                .doOnNext(
                    msg -> {
                      timeContext.stop();
                      calculateLatency(
                          msg,
                          clientToGatewayTimer,
                          gatewayToServiceTimer,
                          serviceToGatewayTimer,
                          gatewayToClientTimer);
                    });
          };
        },
        (state, client) -> client.close());
  }

  private static void calculateLatency(
      ClientMessage message,
      BenchmarksTimer clientToGatewayTimer,
      BenchmarksTimer gatewayToServiceTimer,
      BenchmarksTimer serviceToGatewayTimer,
      BenchmarksTimer gatewayToClientTimer) {

    String clientSendTime = message.header(CLIENT_SEND_TIME);
    String clientRecvTime = message.header(CLIENT_RECV_TIME);
    String gwRecvFromClientTime = message.header(GW_RECV_FROM_CLIENT_TIME);
    String gwRecvFromServiceTime = message.header(GW_RECV_FROM_SERVICE_TIME);
    String serviceRecvTime = message.header(SERVICE_RECV_TIME);

    long clientToGatewayTime =
        Long.parseLong(gwRecvFromClientTime) - Long.parseLong(clientSendTime);
    clientToGatewayTimer.update(clientToGatewayTime, TimeUnit.MILLISECONDS);

    long gatewayToServiceTime =
        Long.parseLong(serviceRecvTime) - Long.parseLong(gwRecvFromClientTime);
    gatewayToServiceTimer.update(gatewayToServiceTime, TimeUnit.MILLISECONDS);

    long serviceToGatewayTime =
        Long.parseLong(gwRecvFromServiceTime) - Long.parseLong(serviceRecvTime);
    serviceToGatewayTimer.update(serviceToGatewayTime, TimeUnit.MILLISECONDS);

    long gatewayToClientTime =
        Long.parseLong(clientRecvTime) - Long.parseLong(gwRecvFromServiceTime);
    gatewayToClientTimer.update(gatewayToClientTime, TimeUnit.MILLISECONDS);
  }
}
