package io.scalecube.services.benchmarks;

import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public final class LatencyHelper {

  private static final String SERVICE_RECV_TIME = "service-recv-time";
  private static final String SERVICE_SEND_TIME = "service-send-time";
  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

  private final BenchmarkTimer clientToServiceTimer;
  private final BenchmarkTimer serviceToClientTimer;

  /**
   * Creates an instance which helps calculate gateway latency by the headers into received message.
   *
   * @param state a benchmark state
   */
  public LatencyHelper(BenchmarkState state) {
    clientToServiceTimer = state.timer("timer.client-to-service");
    serviceToClientTimer = state.timer("timer.service-to-client");
  }

  /**
   * Calculates latencies by the headers into received message.
   *
   * @param message client message
   */
  public void calculate(ServiceMessage message) {
    // client to service
    eval(
        message.header(SERVICE_RECV_TIME),
        message.header(CLIENT_SEND_TIME),
        (v1, v2) -> clientToServiceTimer.update(v1 - v2, TimeUnit.MILLISECONDS));

    // service to client
    eval(
        message.header(CLIENT_RECV_TIME),
        message.header(SERVICE_SEND_TIME),
        (v1, v2) -> serviceToClientTimer.update(v1 - v2, TimeUnit.MILLISECONDS));
  }

  /**
   * Calculates latencies by the headers into received message.
   *
   * @param message client message
   */
  public void calculate(ClientMessage message) {
    // client to service
    eval(
        message.header(SERVICE_RECV_TIME),
        message.header(CLIENT_SEND_TIME),
        (v1, v2) -> clientToServiceTimer.update(v1 - v2, TimeUnit.MILLISECONDS));

    // service to client
    eval(
        message.header(CLIENT_RECV_TIME),
        message.header(SERVICE_SEND_TIME),
        (v1, v2) -> serviceToClientTimer.update(v1 - v2, TimeUnit.MILLISECONDS));
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
