package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static reactor.core.publisher.Mono.from;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ErrorFlowTest {

  @AfterAll
  static void shutdownNodes() {
    StateProvider.close();
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  void testCorruptedRequest(State state) {
    Publisher<ServiceMessage> req =
        state
            .consumer
            .call()
            .create()
            .requestOne(TestRequests.GREETING_CORRUPTED_PAYLOAD_REQUEST, GreetingResponse.class);
    assertThrows(InternalServiceException.class, () -> from(req).block());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  void testNotAuthorized(State state) {
    Publisher<ServiceMessage> req =
        state
            .consumer
            .call()
            .create()
            .requestOne(TestRequests.GREETING_UNAUTHORIZED_REQUEST, GreetingResponse.class);
    assertThrows(UnauthorizedException.class, () -> from(req).block());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  void testNullRequestPayload(State state) {
    Publisher<ServiceMessage> req =
        state
            .consumer
            .call()
            .create()
            .requestOne(TestRequests.GREETING_NULL_PAYLOAD, GreetingResponse.class);
    assertThrows(BadRequestException.class, () -> from(req).block());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  void testServiceUnavailable(State state) {
    StepVerifier.create(state.consumer.call().create().requestOne(TestRequests.NOT_FOUND_REQ))
        .expectError(ServiceUnavailableException.class)
        .verify();
  }

  private static class StateProvider implements ArgumentsProvider {

    private static Map<ServiceTransport, State> states = new ConcurrentHashMap<>();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return ServiceLoaderUtil.findAll(ServiceTransport.class)
          .map(
              serviceTransport ->
                  Arguments.of(states.computeIfAbsent(serviceTransport, State::new)));
    }

    private static void close() {
      states.forEach((serviceTransport, state) -> state.close());
      states.clear();
    }
  }

  private static class State {

    private static AtomicInteger port = new AtomicInteger(4000);

    private final ServiceTransport serviceTransport;
    private final Microservices provider;
    private final Microservices consumer;

    private State(ServiceTransport serviceTransport) {
      this.serviceTransport = serviceTransport;
      provider =
          Objects.requireNonNull(
              Microservices.builder()
                  .transport(options -> options.transport(serviceTransport))
                  .discovery(options -> options.port(port.incrementAndGet()))
                  .services(new GreetingServiceImpl())
                  .startAwait());
      consumer =
          Objects.requireNonNull(
              Microservices.builder()
                  .transport(options -> options.transport(serviceTransport))
                  .discovery(
                      options ->
                          options
                              .seeds(provider.discovery().address())
                              .port(port.incrementAndGet()))
                  .start()
                  .doOnError(ex -> provider.shutdown().subscribe())
                  .block());
    }

    void close() {
      Mono.whenDelayError(consumer.shutdown(), provider.shutdown()).block();
    }

    @Override
    public String toString() {
      return "State{serviceTransport=" + serviceTransport.getClass().getSimpleName() + '}';
    }
  }
}
