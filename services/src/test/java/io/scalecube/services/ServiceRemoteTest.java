package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.sut.BasePojo;
import io.scalecube.services.sut.CoarseGrainedService;
import io.scalecube.services.sut.CoarseGrainedServiceImpl;
import io.scalecube.services.sut.EmptyGreetingRequest;
import io.scalecube.services.sut.EmptyGreetingResponse;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.sut.MyPojo;
import io.scalecube.services.sut.typed.Circle;
import io.scalecube.services.sut.typed.EndOfDayEvent;
import io.scalecube.services.sut.typed.Rectangle;
import io.scalecube.services.sut.typed.Square;
import io.scalecube.services.sut.typed.StartOfDayEvent;
import io.scalecube.services.sut.typed.TradeEvent;
import io.scalecube.services.sut.typed.TypedGreetingService;
import io.scalecube.services.sut.typed.TypedGreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class ServiceRemoteTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static Microservices provider;

  @BeforeAll
  public static void setup() {
    Hooks.onOperatorDebug();
    gateway = gateway();
    gatewayAddress = gateway.discoveryAddress();
    provider = serviceProvider();
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

  private static Microservices serviceProvider() {
    return Microservices.start(
        new Context()
            .discovery(ServiceRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl(), new TypedGreetingServiceImpl()));
  }

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofMillis(500);

    GreetingService service = api(GreetingService.class);

    // call the service.
    Mono<GreetingResponse> result =
        Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", duration)));
    assertEquals(" hello to: joe", result.block(Duration.ofSeconds(10)).result());
  }

  @Test
  public void test_remote_void_greeting() throws Exception {

    GreetingService service = api(GreetingService.class);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(Duration.ofSeconds(3));

    System.out.println("test_remote_void_greeting done.");
  }

  @Test
  public void test_remote_failing_void_greeting() {

    GreetingService service = api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_throwing_void_greeting() {
    GreetingService service = api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.throwingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    Mono<String> future = Mono.from(service.greeting("joe"));
    assertEquals(" hello to: joe", future.block(Duration.ofSeconds(3)));
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());

    assertEquals("hello unknown", future.block(Duration.ofSeconds(1)));
  }

  @Test
  public void test_remote_greeting_no_params_fire_and_forget() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    service.notifyGreeting();
  }

  @Test
  public void test_remote_greeting_return_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(10000)).result());
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    Publisher<GreetingResponse> result =
        service.greetingRequestTimeout(new GreetingRequest("joe", Duration.ofSeconds(4)));

    Mono.from(result)
        .doOnError(
            success -> {
              // print the greeting.
              System.out.println("remote_greeting_request_timeout_expires : " + success);
              assertTrue(success instanceof TimeoutException);
            });
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)).result());
  }

  @Test
  public void test_remote_greeting_message() {
    GreetingService service = api(GreetingService.class);

    ServiceMessage request = ServiceMessage.builder().data(new GreetingRequest("joe")).build();

    // using proxy
    StepVerifier.create(service.greetingMessage(request))
        .assertNext(
            message -> {
              assertEquals(GreetingResponse.class, message.data().getClass());

              GreetingResponse resp = message.data();

              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.result());
            })
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(service.greetingMessage2(request))
        .assertNext(
            resp -> {
              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.result());
            })
        .expectComplete()
        .verify(TIMEOUT);

    // using serviceCall directly
    ServiceCall serviceCall = gateway.call();

    StepVerifier.create(
            serviceCall.requestOne(
                ServiceMessage.from(request).qualifier("v1/greetings/greetingMessage").build(),
                GreetingResponse.class))
        .assertNext(
            message -> {
              assertEquals(GreetingResponse.class, message.data().getClass());

              GreetingResponse resp = message.data();

              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.result());
            })
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(
            serviceCall.requestOne(
                ServiceMessage.from(request).qualifier("v1/greetings/greetingMessage2").build(),
                GreetingResponse.class))
        .assertNext(
            message -> {
              assertEquals(GreetingResponse.class, message.data().getClass());

              GreetingResponse resp = message.data();

              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.result());
            })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_using_setter() {

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.start(
            new Context()
                .discovery(ServiceRemoteTest::serviceDiscovery)
                .transport(RSocketServiceTransport::new)
                .services(new CoarseGrainedServiceImpl()) // add service a and b
            );

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);

    Publisher<String> future = service.callGreeting("joe");

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)));
    provider.close();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.start(
            new Context()
                .discovery(ServiceRemoteTest::serviceDiscovery)
                .transport(RSocketServiceTransport::new)
                .services(another));

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);
    Publisher<String> future = service.callGreeting("joe");
    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)));
    provider.close();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_timeout() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices ms =
        Microservices.start(
            new Context()
                .discovery(ServiceRemoteTest::serviceDiscovery)
                .transport(RSocketServiceTransport::new)
                .services(another) // add service a and b
            );

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);
    InternalServiceException exception =
        assertThrows(
            InternalServiceException.class,
            () -> Mono.from(service.callGreetingTimeout("joe")).block());
    assertTrue(exception.getMessage().contains("Did not observe any item or terminal signal"));
    ms.close();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_dispatcher() {

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices provider =
        Microservices.start(
            new Context()
                .discovery(ServiceRemoteTest::serviceDiscovery)
                .transport(RSocketServiceTransport::new)
                .services(another) // add service a and b
            );

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);

    String response = service.callGreetingWithDispatcher("joe").block(Duration.ofSeconds(5));
    assertEquals(response, " hello to: joe");

    provider.close();
  }

  @Test
  public void test_remote_bidi_greeting_expect_IllegalArgumentException() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service. bidiThrowingGreeting
    Flux<GreetingResponse> responses =
        service.bidiGreetingIllegalArgumentException(
            Mono.just(new GreetingRequest("IllegalArgumentException")));

    // call the service.
    StepVerifier.create(responses)
        .expectErrorMessage("IllegalArgumentException")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_message_expect_IllegalArgumentException() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    // call the service. bidiThrowingGreeting
    Flux<GreetingResponse> responses =
        service
            .bidiGreetingIllegalArgumentExceptionMessage(
                Mono.just(
                    ServiceMessage.builder()
                        .data(new GreetingRequest("IllegalArgumentException"))
                        .build()))
            .map(ServiceMessage::data);

    // call the service.
    StepVerifier.create(responses)
        .expectErrorMessage("IllegalArgumentException")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_expect_NotAuthorized() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    Sinks.Many<GreetingRequest> requests = Sinks.many().multicast().onBackpressureBuffer();
    // call the service.
    Flux<GreetingResponse> responses =
        service.bidiGreetingNotAuthorized(requests.asFlux().onBackpressureBuffer());

    // call the service.

    requests.emitNext(new GreetingRequest("joe-1"), FAIL_FAST);
    requests.emitComplete(FAIL_FAST);

    StepVerifier.create(responses)
        .expectErrorMessage("Not authorized")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_message_expect_NotAuthorized() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    Sinks.Many<GreetingRequest> requests = Sinks.many().multicast().directBestEffort();

    // call the service.
    Flux<GreetingResponse> responses =
        service
            .bidiGreetingNotAuthorizedMessage(
                requests
                    .asFlux()
                    .onBackpressureBuffer()
                    .map(request -> ServiceMessage.builder().data(request).build()))
            .map(ServiceMessage::data);

    StepVerifier.create(responses)
        .then(() -> requests.emitNext(new GreetingRequest("joe-1"), FAIL_FAST))
        .expectErrorMessage("Not authorized")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_expect_GreetingResponse() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    Sinks.Many<GreetingRequest> requests = Sinks.many().multicast().onBackpressureBuffer();

    // call the service.

    requests.emitNext(new GreetingRequest("joe-1"), FAIL_FAST);
    requests.emitNext(new GreetingRequest("joe-2"), FAIL_FAST);
    requests.emitNext(new GreetingRequest("joe-3"), FAIL_FAST);
    requests.emitComplete(FAIL_FAST);

    // call the service.
    Flux<GreetingResponse> responses =
        service.bidiGreeting(requests.asFlux().onBackpressureBuffer());

    StepVerifier.create(responses)
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-1"))
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-2"))
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-3"))
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_message_expect_GreetingResponse() {

    // get a proxy to the service api.
    GreetingService service = api(GreetingService.class);

    Sinks.Many<GreetingRequest> requests = Sinks.many().unicast().onBackpressureBuffer();

    // call the service.
    Flux<GreetingResponse> responses =
        service
            .bidiGreetingMessage(
                requests
                    .asFlux()
                    .onBackpressureBuffer()
                    .map(request -> ServiceMessage.builder().data(request).build()))
            .map(ServiceMessage::data);

    StepVerifier.create(responses)
        .then(() -> requests.emitNext(new GreetingRequest("joe-1"), FAIL_FAST))
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-1"))
        .then(() -> requests.emitNext(new GreetingRequest("joe-2"), FAIL_FAST))
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-2"))
        .then(() -> requests.emitNext(new GreetingRequest("joe-3"), FAIL_FAST))
        .expectNextMatches(resp -> resp.result().equals(" hello to: joe-3"))
        .then(() -> requests.emitComplete(FAIL_FAST))
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_services_contribute_to_cluster_metadata() {
    Map<String, String> tags = new HashMap<>();
    tags.put("HOSTNAME", "host1");

    Microservices ms =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .tags(tags)
                .services(new GreetingServiceImpl()));

    assertTrue(ms.serviceEndpoint().tags().containsKey("HOSTNAME"));
  }

  @Test
  public void test_remote_mono_empty_greeting() {
    GreetingService service = api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingMonoEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_mono_empty_request_response_greeting() {
    GreetingService service = api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.emptyGreeting(new EmptyGreetingRequest()))
        .expectNextMatches(resp -> resp instanceof EmptyGreetingResponse)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_flux_empty_greeting() {
    GreetingService service = api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingFluxEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Disabled("https://github.com/scalecube/scalecube-services/issues/742")
  public void test_many_stream_block_first() {
    GreetingService service = api(GreetingService.class);

    for (int i = 0; i < 100; i++) {
      //noinspection ConstantConditions
      long first = service.manyStream(30L).filter(k -> k != 0).take(1).blockFirst();
      assertEquals(1, first);
    }
  }

  @Test
  public void test_dynamic_qualifier() {
    final var value = "12345";
    final var data = System.currentTimeMillis();
    final var request =
        ServiceMessage.builder().qualifier("v1/greetings/hello/" + value).data(data).build();

    StepVerifier.create(gateway.call().requestOne(request, String.class).map(ServiceMessage::data))
        .assertNext(result -> assertEquals(value + "@" + data, result))
        .verifyComplete();
  }

  @Test
  public void test_generics_in_request() {
    final var service = api(GreetingService.class);
    final var pojo = new MyPojo("Joe", "NY");

    StepVerifier.create(service.greetingsWithGenerics(new BasePojo<MyPojo>().object(pojo)))
        .assertNext(result -> assertEquals(pojo, result))
        .verifyComplete();
  }

  @Test
  public void test_polymorph() {
    final var greetingService = api(TypedGreetingService.class);

    StepVerifier.create(greetingService.helloPolymorph())
        .assertNext(shape -> assertEquals(1.0, ((Circle) shape).radius()))
        .assertNext(
            shape -> {
              assertEquals(1.0, ((Rectangle) shape).height());
              assertEquals(1.0, ((Rectangle) shape).width());
            })
        .assertNext(shape -> assertEquals(1.0, ((Square) shape).side()))
        .thenCancel()
        .verify();
  }

  @Test
  public void test_multitype() {
    final var greetingService = api(TypedGreetingService.class);

    StepVerifier.create(greetingService.helloMultitype())
        .assertNext(
            event -> {
              final var sodEvent = (StartOfDayEvent) event;
              assertEquals(1, sodEvent.timestamp());
              assertEquals(1, sodEvent.trackingNumber());
              assertEquals(1, sodEvent.eventId());
              assertNotNull(sodEvent.sodTime());
            })
        .assertNext(
            event -> {
              final var eodEvent = (EndOfDayEvent) event;
              assertEquals(1, eodEvent.timestamp());
              assertEquals(2, eodEvent.trackingNumber());
              assertEquals(2, eodEvent.eventId());
              assertNotNull(eodEvent.eodTime());
            })
        .assertNext(
            event -> {
              final var executedEvent = (TradeEvent) event;
              assertEquals(1, executedEvent.timestamp());
              assertEquals(3, executedEvent.trackingNumber());
              assertEquals(3, executedEvent.eventId());
              assertEquals(new BigDecimal("100"), executedEvent.price());
              assertEquals(new BigDecimal("100"), executedEvent.quantity());
              assertEquals(100, executedEvent.tradeId());
            })
        .thenCancel()
        .verify();
  }

  @Test
  public void test_wildcard_multitype() {
    final var greetingService = api(TypedGreetingService.class);

    StepVerifier.create(greetingService.helloWildcardMultitype())
        .assertNext(
            event -> {
              final var sodEvent = (StartOfDayEvent) event;
              assertEquals(1, sodEvent.timestamp());
              assertEquals(1, sodEvent.trackingNumber());
              assertEquals(1, sodEvent.eventId());
              assertNotNull(sodEvent.sodTime());
            })
        .assertNext(
            event -> {
              final var eodEvent = (EndOfDayEvent) event;
              assertEquals(1, eodEvent.timestamp());
              assertEquals(2, eodEvent.trackingNumber());
              assertEquals(2, eodEvent.eventId());
              assertNotNull(eodEvent.eodTime());
            })
        .assertNext(
            event -> {
              final var executedEvent = (TradeEvent) event;
              assertEquals(1, executedEvent.timestamp());
              assertEquals(3, executedEvent.trackingNumber());
              assertEquals(3, executedEvent.eventId());
              assertEquals(new BigDecimal("100"), executedEvent.price());
              assertEquals(new BigDecimal("100"), executedEvent.quantity());
              assertEquals(100, executedEvent.tradeId());
            })
        .thenCancel()
        .verify();
  }

  private <T> T api(Class<T> api) {
    return gateway.call().api(api);
  }

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(endpoint))
        .membership(cfg -> cfg.seedMembers(gatewayAddress.toString()));
  }
}
