package io.scalecube.services.benchmarks;

import com.codahale.metrics.Timer;

import java.util.stream.LongStream;

import reactor.core.publisher.Flux;

public class RequestManyBenchmarksRunner {

  public static void main(String[] args) {
    ServicesBenchmarksSettings settings = ServicesBenchmarksSettings.from(args).build();
    ServicesBenchmarksState state = new ServicesBenchmarksState(settings);
    state.setup();

    BenchmarkService benchmarkService = state.service();
    int responseCount = 10;
    BenchmarkMessage message = new BenchmarkMessage(String.valueOf(responseCount));
    Timer timer = state.registry().timer("requestMany" + "-timer");

    Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(state.scheduler())
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return benchmarkService.requestMany(message).doOnEach(next -> timeContext.stop());
        }))
        .take(settings.executionTaskTime())
        .blockLast();

    state.tearDown();
  }
}
