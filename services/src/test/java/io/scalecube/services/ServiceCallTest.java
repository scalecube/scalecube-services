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
import static io.scalecube.services.TestRequests.NOT_FOUND_REQ;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.routing.RoundRobinServiceRouter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ServiceCallTest extends BaseTest {

  public static final int TIMEOUT = 3;
  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private Microservices gateway;

  @BeforeEach
  public void setup() {
    this.gateway = gateway();
  }

  @AfterEach
  public void tearDown() {
    gateway.shutdown().block();
  }

  @Test
  public void test_local_async_no_params() {
    // Create microservices cluster.
    Microservices microservices = serviceProvider();

    Call serviceCall = microservices.call().router(RoundRobinServiceRouter.class);

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.create().requestOne(GREETING_NO_PARAMS_REQUEST);

    ServiceMessage message = Mono.from(future).block(Duration.ofSeconds(TIMEOUT));

    assertEquals( GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier(), "Didn't get desired response");

    microservices.shutdown().block();
  }

  private Microservices serviceProvider() {
    return Microservices.builder()
        .services(new GreetingServiceImpl())
        .startAwait();
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .startAwait();

    Call serviceCall = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.create().requestOne(GREETING_NO_PARAMS_REQUEST, GreetingResponse.class);

    ServiceMessage message = Mono.from(future).block(timeout);

    assertThat(message.data(), instanceOf(GreetingResponse.class));
    assertTrue(((GreetingResponse) message.data()).getResult().equals("hello unknown"));

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_void_greeting() throws Exception {
    // Given

    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    // When
    gateway.call().create().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));

    node1.shutdown().block();
  }

  @Test
  public void test_remote_failing_void_greeting() {

    // Given
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    // When
    StepVerifier.create(gateway.call().create().requestOne(GREETING_FAILING_VOID_REQ, Void.class))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    node1.shutdown().block();
  }

  @Test
  public void test_remote_throwing_void_greeting() {
    // Given
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    // When
    StepVerifier.create(gateway.call().create().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    node1.shutdown().block();
  }

  @Test
  public void test_remote_fail_greeting() {
    // Given
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    // When
    Throwable exception = assertThrows(ServiceException.class,
        () -> Mono.from(gateway.call().create().requestOne(GREETING_FAIL_REQ, GreetingResponse.class)).block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());

    node1.shutdown().block();
  }

  @Test
  public void test_remote_exception_void() {
    // Given
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    // When
    Throwable exception = assertThrows(ServiceException.class,
        () -> Mono.from(gateway.call().create().requestOne(GREETING_ERROR_REQ, GreetingResponse.class)).block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
    node1.shutdown().block();
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    // GIVEN
    Microservices node = serviceProvider();

    // WHEN
    node.call().create().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().block();
  }

  @Test
  public void test_local_failng_void_greeting() throws Exception {
    // GIVEN
    Microservices node = serviceProvider();

    StepVerifier.create(node.call().create().oneWay(GREETING_FAILING_VOID_REQ))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().block();
  }

  @Test
  public void test_local_throwing_void_greeting() throws Exception {
    // GIVEN
    Microservices node = serviceProvider();

    StepVerifier.create(node.call().create().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().block();
  }

  @Test
  public void test_local_fail_greeting() {
    // Create microservices instance.
    Microservices node = serviceProvider();

    // call the service.
    Throwable exception =
        assertThrows(ServiceException.class, () -> Mono.from(node.call().create().requestOne(GREETING_FAIL_REQ)).block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());

    node.shutdown().block();
  }

  @Test
  public void test_local_exception_greeting() {
    // Create microservices instance.
    Microservices node = serviceProvider();

    // call the service.
    Throwable exception = assertThrows(ServiceException.class,
        () -> Mono.from(node.call().create().requestOne(GREETING_ERROR_REQ)).block(timeout));

    node.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .startAwait();

    Publisher<ServiceMessage> resultFuture = consumer.call().create().requestOne(GREETING_REQ, String.class);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(Duration.ofSeconds(TIMEOUT));
    assertNotNull(result);
    assertEquals(GREETING_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", result.data());

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {
    // Given
    Microservices microservices = serviceProvider();

    // When
    Publisher<ServiceMessage> resultFuture =
        microservices.call().create().requestOne(GREETING_REQUEST_REQ);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(Duration.ofSeconds(TIMEOUT));
    assertNotNull(result);
    assertEquals(GREETING_REQUEST_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", ((GreetingResponse) result.data()).getResult());

    microservices.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse() {
    // Given
    Microservices provider = serviceProvider();
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .startAwait();

    // When
    Publisher<ServiceMessage> result =
        consumer.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    // Then
    GreetingResponse greeting = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    assertEquals(" hello to: joe", greeting.getResult());

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    // Given:
    Microservices node = serviceProvider();
    Call service = node.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.create().requestOne(GREETING_REQUEST_TIMEOUT_REQ);

    Throwable exception = assertThrows(RuntimeException.class, () -> {
      try {
        Mono.from(future).block(Duration.ofSeconds(1));
      } finally {
        node.shutdown().block();
      }
    });
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .startAwait();

    Call service = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.create().requestOne(GREETING_REQUEST_TIMEOUT_REQ);
    Throwable exception = assertThrows(RuntimeException.class, () -> Mono.from(future).block(Duration.ofSeconds(1)));
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));

    provider.shutdown().block();
      consumer.shutdown().block();

  }

  // Since here and below tests were not reviewed [sergeyr]

  @Test
  public void test_local_async_greeting_return_Message() throws Exception {
    // Given:
    Microservices microservices = serviceProvider();

    // call the service.

    ServiceMessage result =
        microservices.call().create().requestOne(GREETING_REQUEST_REQ).block(timeout);

    // print the greeting.
    GreetingResponse responseData = result.data();
    System.out.println("local_async_greeting_return_Message :" + responseData);

    assertEquals(" hello to: joe", responseData.getResult());

    microservices.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .startAwait();

    Call service = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.create().requestOne(GREETING_REQUEST_REQ);

    Mono.from(future).doOnNext(result -> {
      // print the greeting.
      System.out.println("10. remote_async_greeting_return_Message :" + result.data());
      // print the greeting.
      assertThat(result.data(), instanceOf(GreetingResponse.class));
      assertTrue(((GreetingResponse) result.data()).getResult().equals(" hello to: joe"));
    });

    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test
  public void test_round_robin_selection_logic() {
    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl(1))
        .startAwait();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl(2))
        .startAwait();

    ServiceCall service = gateway.call().create();

    // call the service.
    GreetingResponse result1 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block()
            .data();
    GreetingResponse result2 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block()
            .data();

    assertTrue(!result1.sender().equals(result2.sender()));
    provider2.shutdown().block();
    provider1.shutdown().block();
  }



  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() throws Exception {
    // Create microservices instance cluster.
    Microservices provider1 = provider(gateway);

    Call service = provider1.call();

    try {
      // call the service.
      Mono.from(service.create().requestOne(NOT_FOUND_REQ)).block(timeout);
      fail("Expected no-service-found exception");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("No reachable member with such service: " + NOT_FOUND_REQ.qualifier()));
    }

    provider1.shutdown().block();
  }


  @Test
  public void test_dispatcher_remote_greeting_request_completes_before_timeout() {
    // Create microservices instance.
    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    Publisher<ServiceMessage> result = gateway.call().create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    System.out.println("greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));

    node.shutdown().block();
  }

  @Test
  public void test_dispatcher_local_greeting_request_completes_before_timeout() {
    Microservices gateway = Microservices.builder()
        .services(new GreetingServiceImpl())
        .startAwait();

    ServiceCall service = gateway.call().create();

    Publisher<ServiceMessage> result = service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).timeout(Duration.ofSeconds(TIMEOUT)).block().data();
    System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));

    gateway.shutdown().block();
  }

  private Microservices provider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .startAwait();
  }

  private Microservices gateway() {
    return Microservices.builder()
        .startAwait();
  }
}
