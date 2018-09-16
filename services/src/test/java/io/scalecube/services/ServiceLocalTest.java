package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceLocalTest extends BaseTest {

  private static final Duration timeout = Duration.ofSeconds(3);

  private static AtomicInteger port = new AtomicInteger(7000);

  private Microservices microservices;

  /** Setup. */
  @BeforeEach
  public void setUp() {
    microservices =
        Microservices.builder()
            .discovery(options -> options.port(port.incrementAndGet()))
            .services(new GreetingServiceImpl())
            .startAwait();
  }

  /** Cleanup. */
  @AfterEach
  public void cleanUp() {
    if (microservices != null) {
      microservices.shutdown().block(timeout);
    }
  }

  @Test
  public void test_local_greeting_request_completes_before_timeout() throws Exception {
    GreetingService service = microservices.call().create().api(GreetingService.class);

    // call the service.
    GreetingResponse result =
        service
            .greetingRequestTimeout(new GreetingRequest("joe", timeout))
            .block(timeout.plusSeconds(1));

    // print the greeting.
    System.out.println("2. greeting_request_completes_before_timeout : " + result.getResult());
    assertEquals(" hello to: joe", result.getResult());
  }

  @Test
  public void test_local_async_greeting() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<String> future = Mono.from(service.greeting("joe"));
    future
        .doOnNext(
            onNext -> {
              assertTrue(onNext.equals(" hello to: joe"));
              // print the greeting.
              System.out.println("3. local_async_greeting :" + onNext);
            })
        .block(Duration.ofSeconds(1000));
  }

  @Test
  public void test_local_no_params() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());
    AtomicReference<String> greetingResponse = new AtomicReference<>();
    future
        .doOnNext(
            (onNext) -> {
              // print the greeting.
              System.out.println("test_local_async_no_params :" + onNext);
              greetingResponse.set(onNext);
            })
        .block(Duration.ofSeconds(1));
    assertEquals("hello unknown", greetingResponse.get());
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    GreetingService service = createProxy(microservices);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(timeout);

    System.out.println("test_local_void_greeting done.");
  }

  @Test
  public void test_local_failing_void_greeting() {
    GreetingService service = createProxy(microservices);

    // call the service.
    GreetingRequest request = new GreetingRequest("joe");
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_local_failing_void_greeting done.");
  }

  @Test
  public void test_local_throwing_void_greeting() {
    GreetingService service = createProxy(microservices);

    // call the service.
    GreetingRequest request = new GreetingRequest("joe");
    StepVerifier.create(service.throwingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_local_throwing_void_greeting done.");
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<GreetingResponse> future = Mono.from(service.greetingRequest(new GreetingRequest("joe")));

    AtomicReference<GreetingResponse> result = new AtomicReference<>();
    future
        .doOnNext(
            onNext -> {
              result.set(onNext);
              System.out.println("remote_async_greeting_return_GreetingResponse :" + onNext);
            })
        .block(Duration.ofSeconds(1));

    assertTrue(result.get().getResult().equals(" hello to: joe"));
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    GreetingService service = createProxy(microservices);

    // call the service.
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", timeout)))
                    .timeout(Duration.ofSeconds(1))
                    .block());
    assertTrue(
        exception.getCause().getMessage().contains("Did not observe any item or terminal signal"));
  }

  @Test
  public void test_local_async_greeting_return_Message() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<GreetingResponse> future = Mono.from(service.greetingRequest(new GreetingRequest("joe")));

    future
        .doOnNext(
            result -> {
              assertTrue(result.getResult().equals(" hello to: joe"));
              // print the greeting.
              System.out.println("9. local_async_greeting_return_Message :" + result);
            })
        .doOnError(System.out::println)
        .block(Duration.ofSeconds(1));
  }

  @Test
  public void test_local_bidi_greeting_expect_IllegalArgumentException() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_expect_NotAuthorized() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_expect_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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

  private GreetingService createProxy(Microservices gateway) {
    return gateway
        .call()
        .create()
        .api(GreetingService.class); // create proxy for GreetingService API
  }
}
