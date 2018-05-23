package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class LocalServiceTest extends BaseTest {

  private static final Duration timeout = Duration.ofSeconds(3);

  private static AtomicInteger port = new AtomicInteger(7000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void test_local_greeting_request_completes_before_timeout() throws Exception {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = node1.call().create().api(GreetingService.class);

    // call the service.
    GreetingResponse result =
        service.greetingRequestTimeout(new GreetingRequest("joe", timeout)).block(timeout.plusSeconds(1));

    // print the greeting.
    System.out.println("2. greeting_request_completes_before_timeout : " + result.getResult());
    assertTrue(result.getResult().equals(" hello to: joe"));

    node1.shutdown();
  }

  @Test
  public void test_local_async_greeting() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<String> future = Mono.from(service.greeting("joe"));
    future.doOnNext(onNext -> {
      assertTrue(onNext.equals(" hello to: joe"));
      // print the greeting.
      System.out.println("3. local_async_greeting :" + onNext);
    }).block(Duration.ofSeconds(1000));
    microservices.shutdown().block();
  }

  @Test
  public void test_local_no_params() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());
    AtomicReference<String> greetingResponse = new AtomicReference<>();
    future.doOnNext((onNext) -> {
      // print the greeting.
      System.out.println("test_local_async_no_params :" + onNext);
      greetingResponse.set(onNext);
    }).block(Duration.ofSeconds(1));
    assertEquals("hello unknown", greetingResponse.get());
    microservices.shutdown().block();
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = node1.call().create().api(GreetingService.class);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe")).block(timeout);

    System.out.println("test_local_void_greeting done.");
    node1.shutdown();
  }

  @Test
  public void test_local_failing_void_greeting() {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = node1.call().create().api(GreetingService.class);

    // call the service.
    GreetingRequest request = new GreetingRequest("joe");
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_local_failing_void_greeting done.");
    node1.shutdown();
  }

  @Test
  public void test_local_throwing_void_greeting() {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = node1.call().create().api(GreetingService.class);

    // call the service.
    GreetingRequest request = new GreetingRequest("joe");
    StepVerifier.create(service.throwingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_local_throwing_void_greeting done.");
    node1.shutdown();
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<GreetingResponse> future = Mono.from(service.greetingRequest(new GreetingRequest("joe")));

    AtomicReference<GreetingResponse> result = new AtomicReference<>();
    future.doOnNext(onNext -> {
      result.set(onNext);
      System.out.println("remote_async_greeting_return_GreetingResponse :" + onNext);
    }).block(Duration.ofSeconds(1));

    assertTrue(result.get().getResult().equals(" hello to: joe"));
    microservices.shutdown().block();
  }


  @Test
  public void test_local_greeting_request_timeout_expires() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Did not observe any item or terminal signal");

    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = node1.call().create().api(GreetingService.class);

    // call the service.

    Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", timeout)))
        .timeout(Duration.ofSeconds(1))
        .block();

    node1.shutdown().block();
  }


  @Test
  public void test_local_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    Mono<GreetingResponse> future = Mono.from(service.greetingRequest(new GreetingRequest("joe")));

    future
        .doOnNext(result -> {
          assertTrue(result.getResult().equals(" hello to: joe"));
          // print the greeting.
          System.out.println("9. local_async_greeting_return_Message :" + result);
        })
        .doOnError(System.out::println)
        .block(Duration.ofSeconds(1));

    microservices.shutdown().block();
  }

  private GreetingService createProxy(Microservices gateway) {
    return gateway.call().create().api(GreetingService.class); // create proxy for GreetingService API
  }

  private boolean await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) throws Exception {
    return timeLatch.await(timeout, timeUnit);
  }
}
