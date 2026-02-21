package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingService.Base;
import io.scalecube.services.sut.GreetingService.MyPojo;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class ServiceLocalTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private Microservices microservices;

  @BeforeEach
  public void setUp() {
    microservices = Microservices.start(new Context().services(new GreetingServiceImpl()));
  }

  @AfterEach
  public void cleanUp() {
    if (microservices != null) {
      microservices.close();
    }
  }

  @Test
  public void test_local_greeting_request_completes_before_timeout() {
    GreetingService service = microservices.call().api(GreetingService.class);

    // call the service.
    GreetingResponse result =
        service
            .greetingRequestTimeout(new GreetingRequest("joe", Duration.ofMillis(500)))
            .block(TIMEOUT.plusMillis(500));

    // print the greeting.
    System.out.println("2. greeting_request_completes_before_timeout : " + result.result());
    assertEquals(" hello to: joe", result.result());
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
              assertEquals(" hello to: joe", onNext);
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
  public void test_local_void_greeting() {
    GreetingService service = createProxy(microservices);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(TIMEOUT);

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

    assertEquals(" hello to: joe", result.get().result());
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    GreetingService service = createProxy(microservices);

    // call the service.
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", TIMEOUT)))
                    .timeout(Duration.ofMillis(500))
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
              assertEquals(" hello to: joe", result.result());
              // print the greeting.
              System.out.println("9. local_async_greeting_return_Message :" + result);
            })
        .doOnError(System.out::println)
        .block(Duration.ofSeconds(1));
  }

  @Test
  public void test_local_greeting_message() {
    GreetingService service = createProxy(microservices);

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
    ServiceCall serviceCall = microservices.call();

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
  public void test_local_bidi_greeting_message_expect_IllegalArgumentException() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_expect_NotAuthorized() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_message_expect_NotAuthorized() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_expect_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_local_bidi_greeting_expect_message_GreetingResponse() {
    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

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
  public void test_dynamic_qualifier() {
    final var value = "12345";
    final var data = System.currentTimeMillis();
    final var request =
        ServiceMessage.builder().qualifier("v1/greetings/hello/" + value).data(data).build();

    StepVerifier.create(
            microservices.call().requestOne(request, String.class).map(ServiceMessage::data))
        .assertNext(result -> assertEquals(value + "@" + data, result))
        .verifyComplete();
  }

  private static GreetingService createProxy(Microservices gateway) {
    return gateway.call().api(GreetingService.class); // create proxy for GreetingService API
  }

  @Test
  public void test_generics_in_request() {
    GreetingService service = createProxy(microservices);

    final var pojo = new MyPojo("Joe", "NY");
    // call the service.
    final Mono<MyPojo> response =
        service.greetingsWithGenerics(new Base<MyPojo>().object(pojo).format("plain"));
    var result = response.block(TIMEOUT.plusMillis(500));

    // print the greeting.
    System.out.println("test_generics_in_request : " + result);
    assertEquals(pojo, result);
  }
}
