package io.scalecube.gateway.benchmarks;

import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_RECV_TIME;
import static io.scalecube.gateway.benchmarks.BenchmarksService.CLIENT_SEND_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public final class RequestOneScenario {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestOneScenario.class);

  private static final String QUALIFIER = "/benchmarks/one";

  private RequestOneScenario() {
    // Do not instantiate
  }

  /**
   * Runner function for benchmarks.
   *
   * @param args program arguments
   * @param benchmarkStateFactory producer function for {@link AbstractBenchmarkState}
   */
  public static void runWith(
      String[] args, Function<BenchmarkSettings, AbstractBenchmarkState<?>> benchmarkStateFactory) {

    int numOfThreads = Runtime.getRuntime().availableProcessors();
    Duration rampUpDuration = Duration.ofSeconds(numOfThreads);

    BenchmarkSettings settings =
        BenchmarkSettings.from(args)
            .injectors(numOfThreads)
            .messageRate(1) // workaround
            .rampUpDuration(rampUpDuration)
            .durationUnit(TimeUnit.MILLISECONDS)
            .build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runWithRampUp(
        (rampUpTick, state) -> state.createClient(),
        state -> {
          LatencyHelper latencyHelper = new LatencyHelper(state);

          return client ->
              (executionTick, task) ->
                  Mono.defer(
                      () -> {
                        Scheduler taskScheduler = task.scheduler();
                        return client
                            .requestResponse(enrichRequest(), taskScheduler)
                            .map(RequestOneScenario::enrichResponse)
                            .doOnError(
                                th -> LOGGER.warn("Exception occured on requestResponse: " + th))
                            .doOnNext(
                                msg -> {
                                  Optional.ofNullable(msg.data())
                                      .ifPresent(ReferenceCountUtil::safestRelease);
                                  latencyHelper.calculate(msg);
                                })
                            .doOnTerminate(() -> taskScheduler.schedule(task));
                      });
        },
        (state, client) -> client.close());
  }

  private static ClientMessage enrichResponse(ClientMessage msg) {
    return ClientMessage.from(msg).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }

  private static ClientMessage enrichRequest() {
    return ClientMessage.builder()
        .qualifier(QUALIFIER)
        .header(CLIENT_SEND_TIME, System.currentTimeMillis())
        .build();
  }
}
