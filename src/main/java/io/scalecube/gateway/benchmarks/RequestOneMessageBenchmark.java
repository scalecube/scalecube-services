package io.scalecube.gateway.benchmarks;

import com.codahale.metrics.Timer;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.benchmarks.example.ExampleService;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RequestOneMessageBenchmark {

  private static final String QUALIFIER = "/" + ExampleService.QUALIFIER + "/oneMessage";
  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private RequestOneMessageBenchmark() {
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

    BenchmarksSettings settings =
        BenchmarksSettings.from(args)
            .injectors(1000)
            .messageRate(100_000)
            .rampUpDuration(Duration.ofSeconds(60))
            .executionTaskDuration(Duration.ofSeconds(300))
            .consoleReporterEnabled(true)
            .durationUnit(TIME_UNIT)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          Timer timer = state.timer("latency.timer");
          Timer clientToGatewayTimer = state.timer("latency.client-to-gw-timer");
          Timer gatewayToServiceTimer = state.timer("latency.gw-to-service-timer");
          Timer serviceToGatewayTimer = state.timer("latency.service-to-gw-timer");
          Timer gatewayToClientTimer = state.timer("latency.gw-to-client-timer");

          return (executionTick, client) -> {
            Timer.Context timeContext = timer.time();
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
      Timer clientToGatewayTimer,
      Timer gatewayToServiceTimer,
      Timer serviceToGatewayTimer,
      Timer gatewayToClientTimer) {
    final Map<String, String> headers = message.headers();

    String clientSendTime = headers.get("client-send-time");
    String gwReceivedFromClientTime = headers.get("gw-recd-from-client-time");
    String serviceReceivedTime = headers.get("srv-recd-time");
    String gwReceivedFromServiceTime = headers.get("gw-recd-from-srv-time");
    String clientReceivedTime = headers.get("client-recd-time");

    if (clientSendTime == null
        || gwReceivedFromClientTime == null
        || gwReceivedFromServiceTime == null
        || serviceReceivedTime == null
        || clientReceivedTime == null) {
      return;
    }

    long clientToGatewayTime =
        Long.parseLong(gwReceivedFromClientTime) - Long.parseLong(clientSendTime);
    clientToGatewayTimer.update(clientToGatewayTime, TIME_UNIT);

    long gatewayToServiceTime =
        Long.parseLong(serviceReceivedTime) - Long.parseLong(gwReceivedFromClientTime);
    gatewayToServiceTimer.update(gatewayToServiceTime, TIME_UNIT);

    long serviceToGatewayTime =
        Long.parseLong(gwReceivedFromServiceTime) - Long.parseLong(serviceReceivedTime);
    serviceToGatewayTimer.update(serviceToGatewayTime, TIME_UNIT);

    long gatewayToClientTime =
        Long.parseLong(clientReceivedTime) - Long.parseLong(gwReceivedFromServiceTime);
    gatewayToClientTimer.update(gatewayToClientTime, TIME_UNIT);
  }
}
