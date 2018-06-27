package io.scalecube.services.benchmarks.services;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class RequestManyCallBenchmarksRunner {

  private static final String RESPONSE_COUNT = "1000";

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).build();
    new ServicesBenchmarksState(settings, new BenchmarkServiceImpl()).runForAsync(state -> {

      ServiceCall serviceCall = state.serviceCall();
      int responseCount = Integer.parseInt(settings.find("responseCount", RESPONSE_COUNT));
      Timer timer = state.timer("timer");
      Meter meter = state.meter("responses");

      ServiceMessage message = ServiceMessage.builder()
          .qualifier(BenchmarkService.class.getName(), "requestMany")
          .data(responseCount)
          .build();

      return i -> {
        Timer.Context timeContext = timer.time();
        return serviceCall.requestMany(message)
            .doOnNext(onNext -> meter.mark())
            .doFinally(next -> timeContext.stop());
      };
    });
  }
}
