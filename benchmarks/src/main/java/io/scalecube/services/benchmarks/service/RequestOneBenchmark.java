package io.scalecube.services.benchmarks.service;

import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_RECV_TIME;
import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_SEND_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;

public class RequestOneBenchmark {

  private static final String QUALIFIER = "/benchmarks/requestOne";

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    BenchmarkSettings settings = BenchmarkSettings.from(args).build();

    new BenchmarkServiceState(settings, new BenchmarkServiceImpl())
        .runForAsync(
            state -> {
              LatencyHelper latencyHelper = new LatencyHelper(state);
              ServiceCall serviceCall = state.serviceCall();

              return i ->
                  serviceCall
                      .requestOne(enrichRequest())
                      .map(RequestOneBenchmark::enrichResponse)
                      .doOnNext(latencyHelper::calculate);
            });
  }

  private static ServiceMessage enrichResponse(ServiceMessage msg) {
    return ServiceMessage.from(msg).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }

  private static ServiceMessage enrichRequest() {
    return ServiceMessage.builder()
        .qualifier(QUALIFIER)
        .header(CLIENT_SEND_TIME, System.currentTimeMillis())
        .build();
  }
}
