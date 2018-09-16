package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_ERROR_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAILING_VOID_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAIL_REQ;
import static io.scalecube.services.TestRequests.GREETING_NO_PARAMS_REQUEST;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_TIMEOUT_REQ;
import static io.scalecube.services.TestRequests.GREETING_THROWING_VOID_REQ;
import static io.scalecube.services.TestRequests.GREETING_VOID_REQ;
import static io.scalecube.services.TestRequests.NOT_FOUND_REQ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallLocalTest extends BaseTest {

  public static final int TIMEOUT = 3;
  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private static Microservices provider;

  /** Setup. */
  @BeforeAll
  public static void setup() {
    provider = serviceProvider();
  }

  /** Cleanup. */
  @AfterAll
  public static void tearDown() {
    provider.shutdown().block();
  }

  @Test
  public void test_local_async_no_params() {

    ServiceCall serviceCall = provider.call().router(RoundRobinServiceRouter.class).create();

    // call the service.
    Publisher<ServiceMessage> future = serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST);

    ServiceMessage message = Mono.from(future).block(Duration.ofSeconds(TIMEOUT));

    assertEquals(
        GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier(), "Didn't get desired response");
  }

  private static Microservices serviceProvider() {
    return Microservices.builder().services(new GreetingServiceImpl()).startAwait();
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    // WHEN
    provider.call().create().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_failng_void_greeting() throws Exception {

    StepVerifier.create(provider.call().create().oneWay(GREETING_FAILING_VOID_REQ))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_throwing_void_greeting() throws Exception {
    StepVerifier.create(provider.call().create().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_fail_greeting() {
    // call the service.
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () -> Mono.from(provider.call().create().requestOne(GREETING_FAIL_REQ)).block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @Test
  public void test_local_exception_greeting() {

    // call the service.
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(provider.call().create().requestOne(GREETING_ERROR_REQ)).block(timeout));
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {

    // When
    Publisher<ServiceMessage> resultFuture =
        provider.call().create().requestOne(GREETING_REQUEST_REQ);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(Duration.ofSeconds(TIMEOUT));
    assertNotNull(result);
    assertEquals(GREETING_REQUEST_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", ((GreetingResponse) result.data()).getResult());
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {

    ServiceCall service = provider.call().create();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);

    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              Mono.from(future).block(Duration.ofSeconds(1));
            });
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  @Test
  public void test_local_async_greeting_return_Message() throws Exception {

    ServiceMessage result =
        provider.call().create().requestOne(GREETING_REQUEST_REQ).block(timeout);

    // print the greeting.
    GreetingResponse responseData = result.data();
    System.out.println("local_async_greeting_return_Message :" + responseData);

    assertEquals(" hello to: joe", responseData.getResult());
  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() throws Exception {

    ServiceCall service = provider.call().create();

    try {
      // call the service.
      Mono.from(service.requestOne(NOT_FOUND_REQ)).block(timeout);
      fail("Expected no-service-found exception");
    } catch (Exception ex) {
      assertEquals(
          ex.getMessage(), "No reachable member with such service: " + NOT_FOUND_REQ.qualifier());
    }
  }
}
