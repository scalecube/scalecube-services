package io.scalecube.services.benchmarks.gateway;

import static io.scalecube.services.examples.BenchmarkService.CLIENT_RECV_TIME;
import static io.scalecube.services.examples.BenchmarkService.CLIENT_SEND_TIME;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.services.benchmarks.LatencyHelper;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import io.scalecube.services.gateway.clientsdk.ReferenceCountUtil;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

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

    BenchmarkSettings settings = BenchmarkSettings.from(args).build();

    AbstractBenchmarkState<?> benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runForAsync(
        state -> {
          LatencyHelper latencyHelper = new LatencyHelper(state);

          BenchmarkMeter clientToServiceMeter = state.meter("meter.client-to-service");
          BenchmarkMeter serviceToClientMeter = state.meter("meter.service-to-client");

          ThreadLocal<Mono<Client>> clientHolder =
              ThreadLocal.withInitial(() -> state.createClient().cache());

          return i -> {
            Mono<Client> clientMono = clientHolder.get();
            return clientMono.flatMap(
                client -> {
                  clientToServiceMeter.mark();
                  return client
                      .requestResponse(enrichRequest())
                      .map(RequestOneScenario::enrichResponse)
                      .doOnNext(
                          msg -> {
                            serviceToClientMeter.mark();
                            Optional.ofNullable(msg.data())
                                .ifPresent(ReferenceCountUtil::safestRelease);
                            latencyHelper.calculate(msg);
                          })
                      .doOnError(th -> LOGGER.warn("Exception occured on requestResponse: " + th));
                });
          };
        });
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
