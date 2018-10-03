package io.scalecube.services.benchmarks.service;

import static io.scalecube.services.benchmarks.service.BenchmarkService.CLIENT_RECV_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
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
            .warmUpDuration(Duration.ofSeconds(30))
            .executionTaskDuration(Duration.ofSeconds(300))
            .build();

    new BenchmarkServiceState(settings, new BenchmarkServiceImpl())
        .runWithRampUp(
            (rampUpTick, state) -> Mono.just(state.service(BenchmarkService.class)),
            state -> {
              LatencyHelper latencyHelper = new LatencyHelper(state);

              Integer rateLimit = rateLimit(settings);

              ServiceMessage message = //
                  ServiceMessage.builder().qualifier(QUALIFIER).build();

              return service ->
                  (executionTick, task) -> {
                    Flux<ServiceMessage> requestStream =
                        service
                            .infiniteStream(message) //
                            .map(InfiniteStreamBenchmark::enichResponse);
                    if (rateLimit != null) {
                      requestStream = requestStream.limitRate(rateLimit);
                    }
                    return requestStream
                        .doOnError(th -> LOGGER.warn("Exception occured on requestStream: " + th))
                        .doOnNext(latencyHelper::calculate);
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
