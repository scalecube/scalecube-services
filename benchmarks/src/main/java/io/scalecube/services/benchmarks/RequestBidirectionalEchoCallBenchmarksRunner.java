package io.scalecube.services.benchmarks;

import static io.scalecube.services.benchmarks.BenchmarkService.REQUEST_BIDIRECTIONAL_ECHO;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;

import com.codahale.metrics.Timer;

import java.util.stream.LongStream;

import reactor.core.publisher.Flux;

public class RequestBidirectionalEchoCallBenchmarksRunner {

  public static void main(String[] args) {
    ServicesBenchmarksSettings settings = ServicesBenchmarksSettings.from(args).build();
    ServicesBenchmarksState state = new ServicesBenchmarksState(settings, new BenchmarkServiceImpl());
    state.setup();

    ServiceCall serviceCall = state.seed().call().create();
    int responseCount = settings.responseCount();
    Timer timer = state.registry().timer("requestBidirectionalEchoCall" + "-timer");

    Flux.merge(Flux.fromStream(LongStream.range(0, Long.MAX_VALUE).boxed())
        .subscribeOn(state.scheduler())
        .map(i -> {
          Timer.Context timeContext = timer.time();
          Flux<ServiceMessage> request = Flux.fromStream(LongStream.range(0, responseCount).boxed())
              .map(j -> REQUEST_BIDIRECTIONAL_ECHO);
          return serviceCall.requestBidirectional(request).doOnNext(next -> timeContext.stop());
        }))
        .take(settings.executionTaskTime())
        .blockLast();

    state.tearDown();
  }
}
