package io.scalecube.services.benchmarks.service;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Flux;

public class RequestManyBenchmark {

  private static final int RESPONSE_COUNT = 1000;
  private static final String RATE_LIMIT = "rateLimit";

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
              BenchmarkService benchmarkService = state.service(BenchmarkService.class);
              int responseCount = getResponseCount(settings);
              Integer rateLimit = getRateLimit(settings);
              BenchmarkTimer timer = state.timer("timer");
              return i -> {
                Context timeContext = timer.time();
                Flux<ServiceMessage> requestStream =
                    benchmarkService.requestStreamRange(responseCount);
                if (rateLimit != null) {
                  requestStream = requestStream.limitRate(rateLimit);
                }
                return requestStream
                    .doOnNext(
                        msg -> {
                          long time = Long.valueOf(msg.header("time"));
                          timer.update(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        })
                    .doFinally(next -> timeContext.stop());
              };
            });
  }

  private static Integer getRateLimit(BenchmarkSettings settings) {
    return Optional.ofNullable(settings.find(RATE_LIMIT, null)).map(Integer::parseInt).orElse(null);
  }

  private static int getResponseCount(BenchmarkSettings settings) {
    return Integer.parseInt(settings.find("responseCount", String.valueOf(RESPONSE_COUNT)));
  }
}
