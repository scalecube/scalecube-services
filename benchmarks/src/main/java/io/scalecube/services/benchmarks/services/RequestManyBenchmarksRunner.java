package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarksSettings;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class RequestManyBenchmarksRunner {

  private static final String RESPONSE_COUNT = "1000";

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl()).runForAsync(state -> {

      BenchmarkService benchmarkService = state.service(BenchmarkService.class);
      int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
      Timer timer = state.timer("timer");
      Meter meter = state.meter("responses");

      return i -> {
        Timer.Context timeContext = timer.time();
        return benchmarkService.requestMany(responseCount)
            .doOnNext(onNext -> meter.mark())
            .doFinally(next -> timeContext.stop());
      };
    });
  }
}
