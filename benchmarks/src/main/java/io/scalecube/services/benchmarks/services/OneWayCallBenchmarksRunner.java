package io.scalecube.services.benchmarks.services;

import static io.scalecube.services.benchmarks.services.BenchmarkService.ONE_WAY;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.ServiceCall;

import com.codahale.metrics.Timer;

public class OneWayCallBenchmarksRunner {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl()).runForAsync(state -> {

      ServiceCall serviceCall = state.serviceCall();
      Timer timer = state.timer("timer");

      return i -> {
        Timer.Context timeContext = timer.time();
        return serviceCall.oneWay(ONE_WAY).doOnTerminate(timeContext::stop);
      };
    });
  }
}
