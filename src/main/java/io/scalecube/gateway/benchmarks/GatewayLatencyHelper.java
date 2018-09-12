package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_RECV_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_SEND_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_CLIENT_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.GW_RECV_FROM_SERVICE_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.SERVICE_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.gateway.clientsdk.ClientMessage;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class GatewayLatencyHelper {

  private final BenchmarkTimer clientToGwTimer;
  private final BenchmarkTimer gwToServiceTimer;
  private final BenchmarkTimer serviceToGwTimer;
  private final BenchmarkTimer gwToClientTimer;

  public GatewayLatencyHelper(BenchmarkState state) {
    clientToGwTimer = state.timer("latency.client-to-gw-timer");
    gwToServiceTimer = state.timer("latency.gw-to-service-timer");
    serviceToGwTimer = state.timer("latency.service-to-gw-timer");
    gwToClientTimer = state.timer("latency.gw-to-client-timer");
  }

  public void calculate(ClientMessage message) {
    // client to gateway
    eval(
        message.header(GW_RECV_FROM_CLIENT_TIME),
        message.header(CLIENT_SEND_TIME),
        (v1, v2) -> clientToGwTimer.update(v1 - v2, TimeUnit.MILLISECONDS));

    // gateway to client
    eval(
        message.header(CLIENT_RECV_TIME),
        message.header(GW_RECV_FROM_SERVICE_TIME),
        (v1, v2) -> gwToClientTimer.update(v1 - v2, TimeUnit.MILLISECONDS));

    // gateway to service
    eval(
        message.header(SERVICE_RECV_TIME),
        message.header(GW_RECV_FROM_CLIENT_TIME),
        (v1, v2) -> gwToServiceTimer.update(v1 - v2, TimeUnit.MILLISECONDS));

    // service to gateway
    eval(
        message.header(GW_RECV_FROM_SERVICE_TIME),
        message.header(SERVICE_RECV_TIME),
        (v1, v2) -> serviceToGwTimer.update(v1 - v2, TimeUnit.MILLISECONDS));
  }

  private void eval(String value0, String value1, BiConsumer<Long, Long> consumer) {
    Optional.ofNullable(value0)
        .map(Long::parseLong)
        .ifPresent(
            long0 ->
                Optional.ofNullable(value1)
                    .map(Long::parseLong)
                    .ifPresent(long1 -> consumer.accept(long0, long1)));
  }
}
