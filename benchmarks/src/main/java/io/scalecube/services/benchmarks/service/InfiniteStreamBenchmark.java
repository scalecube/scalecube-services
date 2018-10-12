package io.scalecube.services.benchmarks.service;

import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class InfiniteStreamBenchmark {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfiniteStreamBenchmark.class);

  private static final String QUALIFIER = "/benchmarks/infiniteStream";

  private static final String RATE_LIMIT = "rateLimit";

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    int numOfThreads = Runtime.getRuntime().availableProcessors();
    Duration rampUpDuration = Duration.ofSeconds(numOfThreads);

    BenchmarkSettings settings =
        BenchmarkSettings.from(args)
            .injectors(numOfThreads)
            .messageRate(1) // workaround
            .rampUpDuration(rampUpDuration)
            .build();

    new BenchmarkServiceState(settings, new BenchmarkServiceImpl())
        .runWithRampUp(
            (rampUpTick, state) -> Mono.just(state.service(BenchmarkService.class)),
            state -> {
              LatencyHelper latencyHelper = new LatencyHelper(state);

              BenchmarkMeter clientToServiceMeter = state.meter("meter.client-to-service");
              BenchmarkMeter serviceToClientMeter = state.meter("meter.service-to-client");

              Integer rateLimit = rateLimit(settings);

              ServiceMessage message = //
                  ServiceMessage.builder().qualifier(QUALIFIER).build();

              return service ->
                  (executionTick, task) -> {
                    clientToServiceMeter.mark();
                    Flux<ServiceMessage> requestStream =
                        service
                            .infiniteStream(message) //
                            .map(InfiniteStreamBenchmark::enichResponse);
                    if (rateLimit != null) {
                      requestStream = requestStream.limitRate(rateLimit);
                    }
                    return requestStream
                        .doOnNext(
                            message1 -> {
                              serviceToClientMeter.mark();
                              latencyHelper.calculate(message1);
                            })
                        .doOnError(ex -> LOGGER.warn("Exception occured: " + ex));
                  };
            },
            (state, service) -> Mono.empty());
  }

  private static Integer rateLimit(BenchmarkSettings settings) {
    return Optional.ofNullable(settings.find(RATE_LIMIT, null)).map(Integer::parseInt).orElse(null);
  }

  private static ServiceMessage enichResponse(ServiceMessage msg) {
    return ServiceMessage.from(msg).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }
}
