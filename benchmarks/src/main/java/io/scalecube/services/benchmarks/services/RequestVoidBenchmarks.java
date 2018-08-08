package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarksSettings;

import com.codahale.metrics.Timer;

public class RequestVoidBenchmarks {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl()).runForAsync(state -> {

      BenchmarkService benchmarkService = state.service(BenchmarkService.class);
      Timer timer = state.timer("timer");

      return i -> {
        Timer.Context timeContext = timer.time();
        return benchmarkService.oneWay("hello").doOnTerminate(timeContext::stop);
      };
    });
  }
}
