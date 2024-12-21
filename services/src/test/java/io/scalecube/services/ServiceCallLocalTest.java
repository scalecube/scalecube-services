package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_EMPTY_REQUEST_RESPONSE;
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

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.sut.EmptyGreetingResponse;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallLocalTest extends BaseTest {

  public static final int TIMEOUT = 3;

  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private static Microservices provider;

  @BeforeAll
  public static void setup() {
    provider = serviceProvider();
  }

  @AfterAll
  public static void tearDown() {
    provider.close();
  }

  @Test
  public void test_local_async_no_params() {

    ServiceCall serviceCall = provider.call().router(RoundRobinServiceRouter.class);

    // call the service.
    Publisher<ServiceMessage> future = serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST);

    ServiceMessage message = Mono.from(future).block(Duration.ofSeconds(TIMEOUT));

    assertEquals(
        GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier(), "Didn't get desired response");
  }

  private static Microservices serviceProvider() {
    return Microservices.start(
        new Context()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl()));
  }

  @Test
  public void test_local_void_greeting() {
    // WHEN
    provider.call().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_failng_void_greeting() {

    StepVerifier.create(provider.call().oneWay(GREETING_FAILING_VOID_REQ))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_throwing_void_greeting() {
    StepVerifier.create(provider.call().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));
  }

  @Test
  public void test_local_fail_greeting() {
    // call the service.
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () -> Mono.from(provider.call().requestOne(GREETING_FAIL_REQ)).block(timeout));
    assertEquals("GreetingRequest{name='joe'}", exception.getMessage());
  }

  @Test
  public void test_local_exception_greeting() {

    // call the service.
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () -> Mono.from(provider.call().requestOne(GREETING_ERROR_REQ)).block(timeout));
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {

    // When
    Publisher<ServiceMessage> resultFuture = provider.call().requestOne(GREETING_REQUEST_REQ);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(Duration.ofSeconds(TIMEOUT));
    assertNotNull(result);
    assertEquals(GREETING_REQUEST_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", ((GreetingResponse) result.data()).getResult());
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {

    ServiceCall service = provider.call();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);

    Throwable exception =
        assertThrows(RuntimeException.class, () -> Mono.from(future).block(Duration.ofMillis(500)));
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  @Test
  public void test_local_async_greeting_return_Message() {

    ServiceMessage result = provider.call().requestOne(GREETING_REQUEST_REQ).block(timeout);

    // print the greeting.
    GreetingResponse responseData = result.data();
    System.out.println("local_async_greeting_return_Message :" + responseData);

    assertEquals(" hello to: joe", responseData.getResult());
  }

  @Test
  public void test_remote_mono_empty_request_response_greeting_messsage() {
    StepVerifier.create(
            provider
                .call()
                .requestOne(GREETING_EMPTY_REQUEST_RESPONSE, EmptyGreetingResponse.class))
        .expectNextMatches(resp -> resp.data() instanceof EmptyGreetingResponse)
        .expectComplete()
        .verify(timeout);
  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() {

    ServiceCall service = provider.call();

    try {
      // call the service.
      Mono.from(service.requestOne(NOT_FOUND_REQ)).block(timeout);
      fail("Expected no-service-found exception");
    } catch (Exception ex) {
      assertEquals(ServiceUnavailableException.class, ex.getClass());
      assertNotNull(ex.getMessage());
      assertTrue(
          ex.getMessage()
              .startsWith("No reachable member with such service: " + NOT_FOUND_REQ.qualifier()));
    }
  }

  @Test
  public void test_custom_error_mapper() {
    GreetingService service =
        new ServiceCall()
            .logger("test_custom_error_mapper", Level.INFO)
            .errorMapper(
                message -> {
                  throw new RuntimeException("custom error mapper");
                })
            .transport(new RSocketServiceTransport().start().clientTransport())
            .router(ServiceCallLocalTest::route)
            .api(GreetingService.class);

    StepVerifier.create(service.exceptionRequest(new GreetingRequest()))
        .expectErrorSatisfies(
            throwable -> {
              Assertions.assertEquals(RuntimeException.class, throwable.getClass());
              Assertions.assertEquals("custom error mapper", throwable.getMessage());
            })
        .verify(timeout);
  }

  private static Optional<ServiceReference> route(
      ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(
        new ServiceReference(
            new ServiceMethodDefinition("dummy"),
            new ServiceRegistration("ns", Collections.emptyMap(), Collections.emptyList()),
            ServiceEndpoint.builder()
                .id(UUID.randomUUID().toString())
                .address(provider.serviceAddress())
                .build()));
  }
}
