package io.scalecube.services.benchmarks;

import io.scalecube.benchmarks.BenchmarksSettings;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class RequestManyLatencyBenchmarksRunner {

  private static final String RESPONSE_COUNT = "1000";

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl()).runForAsync(state -> {

      BenchmarkService benchmarkService = state.service(BenchmarkService.class);
      int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
      Timer timer = state.timer("timer");
      Meter responses = state.meter("responses");
      Histogram latency = state.histogram("latency");

      return i -> {
        Timer.Context timeContext = timer.time();
        return benchmarkService.nanoTime(responseCount)
            .doOnNext(onNext -> {
              latency.update(System.nanoTime() - onNext);
              responses.mark();
            })
            .doFinally(next -> timeContext.stop());
      };
    });
  }
}
