package io.scalecube.services.benchmarks;

import static io.scalecube.services.benchmarks.BenchmarkService.ONE_WAY;

import io.scalecube.services.ServiceCall;

import com.codahale.metrics.Timer;

import java.util.stream.LongStream;

import reactor.core.publisher.Flux;

public class OneWayCallBenchmarksRunner {

  public static void main(String[] args) {
    ServicesBenchmarksSettings settings = ServicesBenchmarksSettings.from(args).build();
    ServicesBenchmarksState state = new ServicesBenchmarksState(settings, new BenchmarkServiceImpl());
    state.setup();

    ServiceCall serviceCall = state.seed().call().create();
    Timer timer = state.registry().timer("oneWayCall" + "-timer");

    Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(state.scheduler())
        .map(i -> {
          Timer.Context timeContext = timer.time();
          return serviceCall.oneWay(ONE_WAY).doOnTerminate(() -> timeContext.stop());
        }))
        .take(settings.executionTaskTime())
        .blockLast();

    state.tearDown();
  }
}
