package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_ERROR_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAILING_VOID_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAIL_REQ;
import static io.scalecube.services.TestRequests.GREETING_NO_PARAMS_REQUEST;
import static io.scalecube.services.TestRequests.GREETING_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_TIMEOUT_REQ;
import static io.scalecube.services.TestRequests.GREETING_THROWING_VOID_REQ;
import static io.scalecube.services.TestRequests.GREETING_VOID_REQ;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import io.scalecube.services.transport.api.ServiceTransport;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallRemoteTest extends BaseTest {

  private Duration timeout = Duration.ofSeconds(10);

  /** Cleanup. */
  @AfterAll
  public static void tearDown() {
    StateProvider.close();
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_async_greeting_no_params(State state) {

    ServiceCall serviceCall = state.gateway.call().create();

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST, GreetingResponse.class);

    ServiceMessage message = Mono.from(future).block(timeout);

    assertTrue(((GreetingResponse) message.data()).getResult().equals("hello unknown"));
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_void_greeting(State state) {
    // When
    StepVerifier.create(state.gateway.call().create().oneWay(GREETING_VOID_REQ))
        .expectComplete()
        .verify(timeout);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_failing_void_greeting(State state) {

    // When
    StepVerifier.create(
            state.gateway.call().create().requestOne(GREETING_FAILING_VOID_REQ, Void.class))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(timeout);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_throwing_void_greeting(State state) {
    // When
    StepVerifier.create(state.gateway.call().create().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(timeout);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_fail_greeting(State state) {
    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(
                        state
                            .gateway
                            .call()
                            .create()
                            .requestOne(GREETING_FAIL_REQ, GreetingResponse.class))
                    .block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_exception_void(State state) {

    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(
                        state
                            .gateway
                            .call()
                            .create()
                            .requestOne(GREETING_ERROR_REQ, GreetingResponse.class))
                    .block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_async_greeting_return_string(State state) {

    Publisher<ServiceMessage> resultFuture =
        state.gateway.call().create().requestOne(GREETING_REQ, String.class);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(timeout);
    assertNotNull(result);
    assertEquals(GREETING_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", result.data());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_async_greeting_return_GreetingResponse(State state) {

    // When
    Publisher<ServiceMessage> result =
        state.gateway.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    // Then
    GreetingResponse greeting = Mono.from(result).block(timeout).data();
    assertEquals(" hello to: joe", greeting.getResult());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_greeting_request_timeout_expires(State state) {

    ServiceCall service = state.gateway.call().create();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);
    Throwable exception =
        assertThrows(RuntimeException.class, () -> Mono.from(future).block(Duration.ofSeconds(1)));
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  // Since here and below tests were not reviewed [sergeyr]
  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_async_greeting_return_Message(State state) {
    ServiceCall service = state.gateway.call().create();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_REQ);

    Mono.from(future)
        .doOnNext(
            result -> {
              // print the greeting.
              System.out.println("10. remote_async_greeting_return_Message :" + result.data());
              // print the greeting.
              assertThat(result.data(), instanceOf(GreetingResponse.class));
              assertTrue(((GreetingResponse) result.data()).getResult().equals(" hello to: joe"));
            });
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_remote_dispatcher_remote_greeting_request_completes_before_timeout(State state) {

    Publisher<ServiceMessage> result =
        state.gateway.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).block(timeout).data();
    System.out.println("greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StateProvider.class)
  public void test_service_address_lookup_occur_only_after_subscription(State state) {

    Flux<ServiceMessage> quotes =
        state
            .gateway
            .call()
            .create()
            .requestMany(
                ServiceMessage.builder()
                    .qualifier(QuoteService.NAME, "onlyOneAndThenNever")
                    .data(null)
                    .build());

    // Add service to cluster AFTER creating a call object.
    // (prove address lookup occur only after subscription)
    Microservices quotesService = state.serviceProvider(new SimpleQuoteService());

    StepVerifier.create(quotes.take(1)).expectNextCount(1).expectComplete().verify(timeout);

    state.close(quotesService);
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

    private final ServiceTransport serviceTransport;

    private final Microservices gateway;
    private final Microservices provider;

    public State(ServiceTransport serviceTransport) {
      this.serviceTransport = serviceTransport;
      gateway =
          Objects.requireNonNull(
              Microservices.builder()
                  .transport(options -> options.transport(serviceTransport))
                  .startAwait());
      provider = Objects.requireNonNull(serviceProvider(new GreetingServiceImpl()));
    }

    void close() {
      try {
        Mono.whenDelayError(gateway.shutdown(), provider.shutdown()).block();
      } catch (Exception e) {
        // no-op
      }
    }

    void close(Microservices serviceProvider) {
      try {
        serviceProvider.shutdown().block();
      } catch (Exception e) {
        // no-op
      }
    }

    private Microservices serviceProvider(Object service) {
      return Microservices.builder()
          .transport(options -> options.transport(serviceTransport))
          .discovery(options -> options.seeds(gateway.discovery().address()))
          .services(service)
          .startAwait();
    }

    @Override
    public String toString() {
      return "State{serviceTransport=" + serviceTransport.getClass().getSimpleName() + '}';
    }
  }
}
