package io.scalecube.services.benchmarks.transport;

import static io.scalecube.services.examples.BenchmarkService.CLIENT_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.benchmarks.LatencyHelper;
import io.scalecube.services.examples.BenchmarkService;
import io.scalecube.services.examples.BenchmarkServiceImpl;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

public class InfiniteStreamBenchmark {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfiniteStreamBenchmark.class);

  private static final String QUALIFIER = "/benchmarks/infiniteStream";

  private static final int DEFAULT_RATE_LIMIT = Queues.SMALL_BUFFER_SIZE;

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

              BenchmarkMeter serviceToClientMeter = state.meter("meter.service-to-client");

              Integer rateLimit = rateLimit(settings);

              ServiceMessage request = ServiceMessage.builder().qualifier(QUALIFIER).build();

              return service ->
                  (executionTick, task) ->
                      service
                          .infiniteStream(request)
                          .map(InfiniteStreamBenchmark::enichResponse)
                          .limitRate(rateLimit)
                          .doOnNext(
                              message -> {
                                serviceToClientMeter.mark();
                                latencyHelper.calculate(message);
                              })
                          .doOnError(ex -> LOGGER.warn("Exception occured: " + ex));
            },
            (state, service) -> Mono.empty());
  }

  private static Integer rateLimit(BenchmarkSettings settings) {
    return Optional.ofNullable(settings.find(RATE_LIMIT, null))
        .map(Integer::parseInt)
        .orElse(DEFAULT_RATE_LIMIT);
  }

  private static ServiceMessage enichResponse(ServiceMessage msg) {
    return ServiceMessage.from(msg).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }
}
