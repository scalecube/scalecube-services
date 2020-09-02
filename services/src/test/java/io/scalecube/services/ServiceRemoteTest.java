package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.sut.CoarseGrainedService;
import io.scalecube.services.sut.CoarseGrainedServiceImpl;
import io.scalecube.services.sut.EmptyGreetingRequest;
import io.scalecube.services.sut.EmptyGreetingResponse;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class ServiceRemoteTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  public static final Duration TIMEOUT2 = Duration.ofSeconds(6);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static Microservices provider;

  @BeforeAll
  public static void setup() {
    Hooks.onOperatorDebug();
    gateway = gateway();
    gatewayAddress = gateway.discovery("gateway").address();
    provider = serviceProvider();
  }

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

  private static Microservices gateway() {
    return Microservices.builder()
        .discovery("gateway", ScalecubeServiceDiscovery::new)
        .transport(RSocketServiceTransport::new)
        .startAwait();
  }

  private static Microservices serviceProvider() {
    return Microservices.builder()
        .discovery("serviceProvider", ServiceRemoteTest::serviceDiscovery)
        .transport(RSocketServiceTransport::new)
        .services(new GreetingServiceImpl())
        .startAwait();
  }

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    GreetingService service = gateway.call().api(GreetingService.class);

    // call the service.
    Mono<GreetingResponse> result =
        Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", duration)));
    assertEquals(" hello to: joe", result.block(Duration.ofSeconds(10)).getResult());
  }

  @Test
  public void test_remote_void_greeting() throws Exception {

    GreetingService service = gateway.call().api(GreetingService.class);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(Duration.ofSeconds(3));

    System.out.println("test_remote_void_greeting done.");

    Thread.sleep(1000);
  }

  @Test
  public void test_remote_failing_void_greeting() {

    GreetingService service = gateway.call().api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_throwing_void_greeting() {
    GreetingService service = gateway.call().api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.throwingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

    // call the service.
    Mono<String> future = Mono.from(service.greeting("joe"));
    assertEquals(" hello to: joe", future.block(Duration.ofSeconds(3)));
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());

    assertEquals("hello unknown", future.block(Duration.ofSeconds(1)));
  }

  @Test
  public void test_remote_greeting_no_params_fire_and_forget() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

    // call the service.
    service.notifyGreeting();
  }

  @Test
  public void test_remote_greeting_return_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(10000)).getResult());
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

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
    GreetingService service = createProxy();

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)).getResult());
  }

  @Test
  void test_remote_greeting_message() {
    GreetingService service = createProxy();

    ServiceMessage request = ServiceMessage.builder().data(new GreetingRequest("joe")).build();

    // using proxy
    StepVerifier.create(service.greetingMessage(request))
        .assertNext(
            message -> {
              assertEquals(GreetingResponse.class, message.data().getClass());

              GreetingResponse resp = message.data();

              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.getResult());
            })
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(service.greetingMessage2(request))
        .assertNext(
            resp -> {
              assertEquals("1", resp.sender());
              assertEquals("hello to: joe", resp.getResult());
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
              assertEquals("hello to: joe", resp.getResult());
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
              assertEquals("hello to: joe", resp.getResult());
            })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_using_setter() {

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.builder()
            .discovery("provider", ServiceRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(new CoarseGrainedServiceImpl()) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);

    Publisher<String> future = service.callGreeting("joe");

    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)));
    provider.shutdown().then(Mono.delay(TIMEOUT2)).block();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.builder()
            .discovery("provider", ServiceRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(another)
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);
    Publisher<String> future = service.callGreeting("joe");
    assertEquals(" hello to: joe", Mono.from(future).block(Duration.ofSeconds(1)));
    provider.shutdown().then(Mono.delay(TIMEOUT2)).block();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_timeout() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices ms =
        Microservices.builder()
            .discovery("ms", ServiceRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(another) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);
    InternalServiceException exception =
        assertThrows(
            InternalServiceException.class,
            () -> Mono.from(service.callGreetingTimeout("joe")).block());
    assertTrue(exception.getMessage().contains("Did not observe any item or terminal signal"));
    System.out.println("done");
    ms.shutdown().then(Mono.delay(TIMEOUT2)).block();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_dispatcher() {

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices provider =
        Microservices.builder()
            .discovery("provider", ServiceRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(another) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().api(CoarseGrainedService.class);

    String response = service.callGreetingWithDispatcher("joe").block(Duration.ofSeconds(5));
    assertEquals(response, " hello to: joe");

    provider.shutdown().then(Mono.delay(TIMEOUT2)).block();
  }

  @Test
  public void test_remote_bidi_greeting_expect_IllegalArgumentException() {

    // get a proxy to the service api.
    GreetingService service = createProxy();

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
    GreetingService service = createProxy();

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
    GreetingService service = createProxy();

    EmitterProcessor<GreetingRequest> requests = EmitterProcessor.create();
    // call the service.
    Flux<GreetingResponse> responses = service.bidiGreetingNotAuthorized(requests);

    // call the service.

    requests.onNext(new GreetingRequest("joe-1"));
    requests.onComplete();

    StepVerifier.create(responses)
        .expectErrorMessage("Not authorized")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_message_expect_NotAuthorized() {

    // get a proxy to the service api.
    GreetingService service = createProxy();

    DirectProcessor<GreetingRequest> requests = DirectProcessor.create();

    // call the service.
    Flux<GreetingResponse> responses =
        service
            .bidiGreetingNotAuthorizedMessage(
                requests.map(request -> ServiceMessage.builder().data(request).build()))
            .map(ServiceMessage::data);

    StepVerifier.create(responses)
        .then(() -> requests.onNext(new GreetingRequest("joe-1")))
        .expectErrorMessage("Not authorized")
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_expect_GreetingResponse() {

    // get a proxy to the service api.
    GreetingService service = createProxy();

    EmitterProcessor<GreetingRequest> requests = EmitterProcessor.create();

    // call the service.

    requests.onNext(new GreetingRequest("joe-1"));
    requests.onNext(new GreetingRequest("joe-2"));
    requests.onNext(new GreetingRequest("joe-3"));
    requests.onComplete();

    // call the service.
    Flux<GreetingResponse> responses = service.bidiGreeting(requests);

    StepVerifier.create(responses)
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-1"))
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-2"))
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-3"))
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_bidi_greeting_message_expect_GreetingResponse() {

    // get a proxy to the service api.
    GreetingService service = createProxy();

    UnicastProcessor<GreetingRequest> requests = UnicastProcessor.create();

    // call the service.
    Flux<GreetingResponse> responses =
        service
            .bidiGreetingMessage(
                requests.map(request -> ServiceMessage.builder().data(request).build()))
            .map(ServiceMessage::data);

    StepVerifier.create(responses)
        .then(() -> requests.onNext(new GreetingRequest("joe-1")))
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-1"))
        .then(() -> requests.onNext(new GreetingRequest("joe-2")))
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-2"))
        .then(() -> requests.onNext(new GreetingRequest("joe-3")))
        .expectNextMatches(resp -> resp.getResult().equals(" hello to: joe-3"))
        .then(() -> requests.onComplete())
        .expectComplete()
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_services_contribute_to_cluster_metadata() {
    Map<String, String> tags = new HashMap<>();
    tags.put("HOSTNAME", "host1");

    Microservices ms =
        Microservices.builder()
            .discovery("ms", ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .tags(tags)
            .services(new GreetingServiceImpl())
            .startAwait();

    assertTrue(ms.serviceEndpoint().tags().containsKey("HOSTNAME"));
  }

  @Test
  public void test_remote_mono_empty_greeting() {
    GreetingService service = gateway.call().api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingMonoEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_mono_empty_request_response_greeting() {
    GreetingService service = gateway.call().api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.emptyGreeting(new EmptyGreetingRequest()))
        .expectNextMatches(resp -> resp instanceof EmptyGreetingResponse)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_flux_empty_greeting() {
    GreetingService service = gateway.call().api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingFluxEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Disabled("https://github.com/scalecube/scalecube-services/issues/742")
  public void test_many_stream_block_first() {
    GreetingService service = gateway.call().api(GreetingService.class);

    for (int i = 0; i < 100; i++) {
      //noinspection ConstantConditions
      long first = service.manyStream(30L).filter(k -> k != 0).take(1).blockFirst();
      assertEquals(1, first);
    }
  }

  private GreetingService createProxy() {
    return gateway.call().api(GreetingService.class); // create proxy for GreetingService API
  }

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery(endpoint)
        .membership(cfg -> cfg.seedMembers(gatewayAddress));
  }
}
