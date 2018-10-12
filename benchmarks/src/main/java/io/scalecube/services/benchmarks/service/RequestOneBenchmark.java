package io.scalecube.services.benchmarks.service;

import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_RECV_TIME;
import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_SEND_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestOneBenchmark {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestOneBenchmark.class);

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

              BenchmarkMeter clientToServiceMeter = state.meter("meter.client-to-service");
              BenchmarkMeter serviceToClientMeter = state.meter("meter.service-to-client");

              ServiceCall serviceCall = state.serviceCall();

              return i -> {
                clientToServiceMeter.mark();
                return serviceCall
                    .requestOne(enrichRequest())
                    .map(RequestOneBenchmark::enrichResponse)
                    .doOnNext(
                        message -> {
                          serviceToClientMeter.mark();
                          latencyHelper.calculate(message);
                        })
                    .doOnError(ex -> LOGGER.warn("Exception occured: " + ex));
              };
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
