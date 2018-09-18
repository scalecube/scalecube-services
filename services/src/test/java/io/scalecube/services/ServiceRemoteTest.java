package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.sut.CoarseGrainedService;
import io.scalecube.services.sut.CoarseGrainedServiceImpl;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceRemoteTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Microservices gateway;
  private static Microservices provider;

  /** Setup. */
  @BeforeAll
  public static void setup() {
    gateway = gateway();
    provider = serviceProvider();
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

  private static Microservices gateway() {
    return Microservices.builder().startAwait();
  }

  private static Microservices serviceProvider() {
    return Microservices.builder()
        .discovery(options -> options.seeds(gateway.discovery().address()))
        .services(new GreetingServiceImpl())
        .startAwait();
  }

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    GreetingService service = gateway.call().create().api(GreetingService.class);

    // call the service.
    Mono<GreetingResponse> result =
        Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", duration)));
    assertTrue(" hello to: joe".equals(result.block(Duration.ofSeconds(10)).getResult()));
  }

  @Test
  public void test_remote_void_greeting() throws Exception {

    GreetingService service = gateway.call().create().api(GreetingService.class);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(Duration.ofSeconds(3));

    System.out.println("test_remote_void_greeting done.");

    Thread.sleep(1000);
  }

  @Test
  public void test_remote_failing_void_greeting() throws Exception {

    GreetingService service = gateway.call().create().api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));
  }

  @Test
  public void test_remote_throwing_void_greeting() throws Exception {
    GreetingService service = gateway.call().create().api(GreetingService.class);

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
    assertTrue(" hello to: joe".equals(future.block(Duration.ofSeconds(3))));
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // get a proxy to the service api.
    GreetingService service = createProxy();

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());

    assertTrue("hello unknown".equals(future.block(Duration.ofSeconds(1))));
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

    assertTrue(
        " hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(10000)).getResult()));
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

    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1)).getResult()));
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_using_setter() {

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(new CoarseGrainedServiceImpl()) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);

    Publisher<String> future = service.callGreeting("joe");

    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1))));
    provider.shutdown().block();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(another)
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);
    Publisher<String> future = service.callGreeting("joe");
    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1))));
    provider.shutdown().block();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_timeout() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices ms =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(another) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);
    InternalServiceException exception =
        assertThrows(
            InternalServiceException.class,
            () -> Mono.from(service.callGreetingTimeout("joe")).block());
    assertTrue(exception.getMessage().contains("Did not observe any item or terminal signal"));
    System.out.println("done");
    ms.shutdown();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_dispatcher() throws Exception {

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    // Create microservices instance cluster.
    Microservices provider =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(another) // add service a and b
            .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);

    String response = service.callGreetingWithDispatcher("joe").block(Duration.ofSeconds(5));
    assertEquals(response, " hello to: joe");

    provider.shutdown().block();
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
  public void test_services_contribute_to_cluster_metadata() {
    Map<String, String> tags = new HashMap<>();
    tags.put("HOSTNAME", "host1");

    Microservices ms =
        Microservices.builder().tags(tags).services(new GreetingServiceImpl()).startAwait();

    assertTrue(ms.discovery().endpoint().tags().containsKey("HOSTNAME"));
  }

  @Test
  public void test_remote_mono_empty_greeting() {
    GreetingService service = gateway.call().create().api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingMonoEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void test_remote_flux_empty_greeting() {
    GreetingService service = gateway.call().create().api(GreetingService.class);

    // call the service.
    StepVerifier.create(service.greetingFluxEmpty(new GreetingRequest("empty")))
        .expectComplete()
        .verify(TIMEOUT);
  }

  private GreetingService createProxy() {
    return gateway
        .call()
        .create()
        .api(GreetingService.class); // create proxy for GreetingService API
  }
}
