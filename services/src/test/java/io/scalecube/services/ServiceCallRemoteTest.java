package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_EMPTY_REQUEST_RESPONSE;
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

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.sut.EmptyGreetingResponse;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallRemoteTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Microservices provider;

  @BeforeAll
  public static void setup() {
    gateway = gateway();
    provider = serviceProvider(new GreetingServiceImpl());
  }

  @AfterAll
  public static void tearDown() {
    try {
      gateway.close();
    } catch (Exception ignore) {
      // no-op
    }

    try {
      provider.close();
    } catch (Exception ignore) {
      // no-op
    }
  }

  private static Microservices serviceProvider(Object service) {
    return Microservices.start(
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(cfg -> cfg.seedMembers(gateway.discoveryAddress().toString())))
            .transport(RSocketServiceTransport::new)
            .services(service));
  }

  @Test
  public void test_remote_async_greeting_no_params() {

    ServiceCall serviceCall = gateway.call();

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST, GreetingResponse.class);

    ServiceMessage message = Mono.from(future).block(TIMEOUT);

    assertEquals("hello unknown", ((GreetingResponse) message.data()).result());
  }

  @Test
  public void test_remote_void_greeting() {
    // When
    StepVerifier.create(gateway.call().oneWay(GREETING_VOID_REQ)).expectComplete().verify(TIMEOUT);
  }

  @Test
  public void test_remote_mono_empty_request_response_greeting_messsage() {
    StepVerifier.create(
            gateway.call().requestOne(GREETING_EMPTY_REQUEST_RESPONSE, EmptyGreetingResponse.class))
        .expectNextMatches(resp -> resp.data() instanceof EmptyGreetingResponse)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_failing_void_greeting() {

    // When
    StepVerifier.create(gateway.call().requestOne(GREETING_FAILING_VOID_REQ, Void.class))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_throwing_void_greeting() {
    // When
    StepVerifier.create(gateway.call().oneWay(GREETING_THROWING_VOID_REQ))
        .expectErrorMessage(GREETING_THROWING_VOID_REQ.data().toString())
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_fail_greeting() {
    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(gateway.call().requestOne(GREETING_FAIL_REQ, GreetingResponse.class))
                    .block(TIMEOUT));
    assertEquals("GreetingRequest[name='joe', duration=null]", exception.getMessage());
  }

  @Test
  public void test_remote_exception_void() {

    // When
    Throwable exception =
        assertThrows(
            ServiceException.class,
            () ->
                Mono.from(gateway.call().requestOne(GREETING_ERROR_REQ, GreetingResponse.class))
                    .block(TIMEOUT));
    assertEquals("GreetingRequest[name='joe', duration=null]", exception.getMessage());
  }

  @Test
  public void test_remote_async_greeting_return_string() {

    Publisher<ServiceMessage> resultFuture = gateway.call().requestOne(GREETING_REQ, String.class);

    // Then
    ServiceMessage result = Mono.from(resultFuture).block(TIMEOUT);
    assertNotNull(result);
    assertEquals(GREETING_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", result.data());
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse() {

    // When
    Publisher<ServiceMessage> result =
        gateway.call().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    // Then
    GreetingResponse greeting = Mono.from(result).block(TIMEOUT).data();
    assertEquals(" hello to: joe", greeting.result());
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {

    ServiceCall service = gateway.call();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);
    Throwable exception =
        assertThrows(RuntimeException.class, () -> Mono.from(future).block(Duration.ofMillis(500)));
    assertTrue(exception.getMessage().contains("Timeout on blocking read"));
  }

  // Since here and below tests were not reviewed [sergeyr]
  @Test
  public void test_remote_async_greeting_return_Message() {
    ServiceCall service = gateway.call();

    // call the service.
    Publisher<ServiceMessage> future = service.requestOne(GREETING_REQUEST_REQ);

    Mono.from(future)
        .doOnNext(
            result -> {
              // print the greeting.
              System.out.println("10. remote_async_greeting_return_Message :" + result.data());
              // print the greeting.
              assertThat(result.data(), instanceOf(GreetingResponse.class));
              assertEquals(" hello to: joe", ((GreetingResponse) result.data()).result());
            });
  }

  @Test
  public void test_remote_dispatcher_remote_greeting_request_completes_before_timeout() {

    Publisher<ServiceMessage> result =
        gateway.call().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).block(TIMEOUT).data();
    System.out.println("greeting_request_completes_before_timeout : " + greetings.result());
    assertEquals(" hello to: joe", greetings.result());
  }

  @Test
  public void test_service_address_lookup_occur_only_after_subscription() {

    Flux<ServiceMessage> quotes =
        gateway
            .call()
            .requestMany(
                ServiceMessage.builder()
                    .qualifier(QuoteService.NAME, "onlyOneAndThenNever")
                    .data(null)
                    .build());

    // Add service to cluster AFTER creating a call object.
    // (prove address lookup occur only after subscription)
    Microservices quotesService = serviceProvider(new SimpleQuoteService());

    StepVerifier.create(quotes.take(1)).expectNextCount(1).expectComplete().verify(TIMEOUT);

    try {
      quotesService.close();
    } catch (Exception ignored) {
      // no-op
    }
  }

  @Disabled("https://github.com/scalecube/scalecube-services/issues/742")
  public void test_many_stream_block_first() {
    ServiceCall call = gateway.call();

    ServiceMessage request = TestRequests.GREETING_MANY_STREAM_30;

    for (int i = 0; i < 100; i++) {
      //noinspection ConstantConditions
      long first =
          call.requestMany(request, Long.class)
              .map(ServiceMessage::<Long>data)
              .filter(k -> k != 0)
              .take(1)
              .blockFirst();
      assertEquals(1, first);
    }
  }

  @Test
  public void test_custom_error_mapper() {
    GreetingService service =
        new ServiceCall()
            .errorMapper(
                message -> {
                  throw new RuntimeException("custom error mapper");
                })
            .transport(new RSocketServiceTransport().start().clientTransport())
            .router(ServiceCallRemoteTest::route)
            .api(GreetingService.class);

    StepVerifier.create(service.exceptionRequest(new GreetingRequest()))
        .expectErrorSatisfies(
            throwable -> {
              Assertions.assertEquals(RuntimeException.class, throwable.getClass());
              Assertions.assertEquals("custom error mapper", throwable.getMessage());
            })
        .verify(TIMEOUT);
  }

  private static Optional<ServiceReference> route(
      ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(
        new ServiceReference(
            ServiceMethodDefinition.fromAction("dummy"),
            new ServiceRegistration("ns", Collections.emptyMap(), Collections.emptyList()),
            ServiceEndpoint.builder()
                .id(UUID.randomUUID().toString())
                .name("app-service-" + System.currentTimeMillis())
                .address(provider.serviceAddress())
                .build()));
  }

  private static Microservices gateway() {
    return Microservices.start(
        new Context()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new));
  }
}
