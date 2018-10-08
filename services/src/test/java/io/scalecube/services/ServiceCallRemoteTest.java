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
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallRemoteTest extends BaseTest {

  public static final int TIMEOUT = 3;
  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private static Microservices gateway;
  private static Microservices provider;

  /** Setup. */
  @BeforeAll
  public static void setup() {
    gateway = gateway();
    provider = serviceProvider(new GreetingServiceImpl());
  }

  /** Cleanup. */
  @AfterAll
  public static void tearDown() {
    try {
      gateway.shutdown().block();
    } catch (Exception ignore) {
      // no-op
    }

    try {
      provider.shutdown().block();
    } catch (Exception ignore) {
      // no-op
    }
  }

  private static Microservices serviceProvider(Object service) {
    return Microservices.builder()
        .discovery(options -> options.seeds(gateway.discovery().address()))
        .services(service)
        .startAwait();
  }

  @Test
  public void test_remote_async_greeting_no_params() {

    ServiceCall serviceCall = gateway.call().create();

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST, GreetingResponse.class);

    ServiceMessage message = Mono.from(future).block(timeout);

    assertTrue(((GreetingResponse) message.data()).getResult().equals("hello unknown"));
  }

  @Test
  public void test_remote_void_greeting() {
    // When
    StepVerifier.create(gateway.call().create().oneWay(GREETING_VOID_REQ))
        .expectComplete()
        .verify(timeout);
  }

  @Test
  public void test_remote_failing_void_greeting() {

    // When
    StepVerifier.create(gateway.call().create().requestOne(GREETING_FAILING_VOID_REQ, Void.class))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_remote_throwing_void_greeting() {
    // When
    StepVerifier.create(gateway.call().create().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_remote_fail_greeting() {
    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(
                        gateway
                            .call()
                            .create()
                            .requestOne(GREETING_FAIL_REQ, GreetingResponse.class))
                    .block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @Test
  public void test_remote_exception_void() {

    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(
                        gateway
                            .call()
                            .create()
                            .requestOne(GREETING_ERROR_REQ, GreetingResponse.class))
                    .block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @Test
  public void test_remote_async_greeting_return_string() {

    Publisher<ServiceMessage> resultFuture =
        gateway.call().create().requestOne(GREETING_REQ, String.class);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(Duration.ofSeconds(TIMEOUT));
    assertNotNull(result);
    assertEquals(GREETING_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", result.data());
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse() {

    // When
    Publisher<ServiceMessage> result =
        gateway.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    // Then
    GreetingResponse greeting = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    assertEquals(" hello to: joe", greeting.getResult());
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {

    ServiceCall service = gateway.call().create();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);
    Throwable exception =
        assertThrows(RuntimeException.class, () -> Mono.from(future).block(Duration.ofSeconds(1)));
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  // Since here and below tests were not reviewed [sergeyr]
  @Test
  public void test_remote_async_greeting_return_Message() {
    ServiceCall service = gateway.call().create();

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

  @Test
  public void test_remote_dispatcher_remote_greeting_request_completes_before_timeout() {

    Publisher<ServiceMessage> result =
        gateway.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    System.out.println("greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));
  }

  @Test
  public void test_service_address_lookup_occur_only_after_subscription() {

    Flux<ServiceMessage> quotes =
        gateway
            .call()
            .create()
            .requestMany(
                ServiceMessage.builder()
                    .qualifier(QuoteService.NAME, "onlyOneAndThenNever")
                    .data(null)
                    .build());

    // Add service to cluster AFTER creating a call object.
    // (prove address lookup occur only after subscription)
    Microservices quotesService = serviceProvider(new SimpleQuoteService());

    StepVerifier.create(quotes.take(1)).expectNextCount(1).expectComplete().verify(timeout);

    try {
      quotesService.shutdown();
    } catch (Exception ignored) {
      // no-op
    }
  }

  private static Microservices gateway() {
    return Microservices.builder().startAwait();
  }
}
