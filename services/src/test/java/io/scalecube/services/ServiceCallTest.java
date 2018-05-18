package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_ERROR_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAILING_VOID_REQ;
import static io.scalecube.services.TestRequests.GREETING_FAIL_REQ;
import static io.scalecube.services.TestRequests.GREETING_NO_PARAMS_REQUEST;
import static io.scalecube.services.TestRequests.GREETING_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ2;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_TIMEOUT_REQ;
import static io.scalecube.services.TestRequests.GREETING_VOID_REQ;
import static io.scalecube.services.TestRequests.NOT_FOUND_REQ;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.a.b.testing.CanaryService;
import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.routing.RoundRobinServiceRouter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ServiceCallTest extends BaseTest {

  public static final int TIMEOUT = 3;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Duration timeout = Duration.ofSeconds(TIMEOUT);



  private static AtomicInteger port = new AtomicInteger(4000);


  @Test
  public void test_local_async_no_params() {
    // Create microservices cluster.
    Microservices microservices = serviceProvider();

    Call serviceCall = microservices.call().router(RoundRobinServiceRouter.class);

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST);

    ServiceMessage message = Mono.from(future).block(Duration.ofSeconds(TIMEOUT));

    assertEquals("Didn't get desired response", GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier());

    microservices.shutdown().block();
  }

  private Microservices serviceProvider() {
    return Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    Call serviceCall = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        serviceCall.requestOne(GREETING_NO_PARAMS_REQUEST, GreetingResponse.class);

    ServiceMessage message = Mono.from(future).block(timeout);

    assertThat(message.data(), instanceOf(GreetingResponse.class));
    assertTrue(((GreetingResponse) message.data()).getResult().equals("hello unknown"));

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_void_greeting() throws Exception {
    // Given
    Microservices gateway = gateway();

    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // When
    gateway.call().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));

    gateway.shutdown().block();
    node1.shutdown().block();
  }

  @Test
  public void test_remote_failing_void_greeting() {
    // Given
    Microservices gateway = gateway();

    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // When
    StepVerifier.create(gateway.call().oneWay(GREETING_FAILING_VOID_REQ))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    gateway.shutdown().block();
    node1.shutdown().block();
  }

  @Test
  public void test_remote_fail_greeting() {
    thrown.expect(ServiceException.class);
    thrown.expectMessage("GreetingRequest{name='joe'}");

    // Given
    Microservices gateway = gateway();

    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // When
    Mono.from(gateway.call().requestOne(GREETING_FAIL_REQ, GreetingResponse.class)).block(timeout);

    gateway.shutdown().block();
    node1.shutdown().block();
  }

  @Test
  public void test_remote_exception_void() {
    thrown.expect(ServiceException.class);
    thrown.expectMessage("GreetingRequest{name='joe'}");

    // Given
    Microservices gateway = gateway();

    Microservices node1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // When
    Mono.from(gateway.call().requestOne(GREETING_ERROR_REQ, GreetingResponse.class)).block(timeout);

    gateway.shutdown().block();
    node1.shutdown().block();
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    // GIVEN
    Microservices node = serviceProvider();

    // WHEN
    node.call().oneWay(GREETING_VOID_REQ).block(Duration.ofSeconds(TIMEOUT));

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().block();
  }

  @Test
  public void test_local_failng_void_greeting() throws Exception {
    // GIVEN
    Microservices node = serviceProvider();

    StepVerifier.create(node.call().oneWay(GREETING_FAILING_VOID_REQ))
        .expectErrorMessage(GREETING_FAILING_VOID_REQ.data().toString())
        .verify(Duration.ofSeconds(TIMEOUT));

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().block();
  }


  @Test
  public void test_local_fail_greeting() {
    thrown.expect(ServiceException.class);
    thrown.expectMessage("GreetingRequest{name='joe'}");

    // Create microservices instance.
    Microservices node = serviceProvider();

    // call the service.
    Mono.from(node.call().requestOne(GREETING_FAIL_REQ)).block(timeout);

    node.shutdown().block();
  }

  @Test
  public void test_local_exception_greeting() {
    thrown.expect(ServiceException.class);
    thrown.expectMessage("GreetingRequest{name='joe'}");

    // Create microservices instance.
    Microservices node = serviceProvider();

    // call the service.
    Mono.from(node.call().requestOne(GREETING_ERROR_REQ)).block(timeout);

    node.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    Publisher<ServiceMessage> resultFuture = consumer.call().requestOne(GREETING_REQ, String.class);

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
        microservices.call().requestOne(GREETING_REQUEST_REQ);

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
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // When
    Publisher<ServiceMessage> result =
        consumer.call().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    // Then
    GreetingResponse greeting = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    assertEquals(" hello to: joe", greeting.getResult());

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Timeout on blocking read");

    // Given:
    Microservices node = serviceProvider();
    Call service = node.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);

    try {
      Mono.from(future).block(Duration.ofSeconds(1));
    } finally {
      node.shutdown().block();
    }
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Timeout on blocking read");

    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    Call service = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.requestOne(GREETING_REQUEST_TIMEOUT_REQ);
    try {
      Mono.from(future).block(Duration.ofSeconds(1));
    } finally {
      provider.shutdown().block();
      consumer.shutdown().block();
    }
  }

  // Since here and below tests were not reviewed [sergeyr]

  @Test
  public void test_local_async_greeting_return_Message() throws Exception {
    // Given:
    Microservices microservices = serviceProvider();

    Call service = microservices.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.requestOne(GREETING_REQUEST_REQ);


    CountDownLatch timeLatch = new CountDownLatch(1);
    Mono.from(future).doOnNext(result -> {

      assertTrue(result.data().equals(" hello to: joe"));
      // print the greeting.
      System.out.println("9. local_async_greeting_return_Message :" + result.data());

      timeLatch.countDown();
    });
    TimeUnit.SECONDS.sleep(1);
    microservices.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    Call service = consumer.call();

    // call the service.
    Publisher<ServiceMessage> future =
        service.requestOne(GREETING_REQUEST_REQ);

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
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl(1))
        .build()
        .startAwait();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl(2))
        .build()
        .startAwait();

    Call service = gateway.call();

    // call the service.
    GreetingResponse result1 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block().data();
    GreetingResponse result2 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block().data();

    assertTrue(!result1.sender().equals(result2.sender()));
    provider2.shutdown().block();
    provider1.shutdown().block();
    gateway.shutdown().block();
  }


  @Test
  public void test_tag_selection_logic() {
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .service(new GreetingServiceImpl(1)).tag("SENDER", "1").register()
        .build()
        .startAwait();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .service(new GreetingServiceImpl(2)).tag("SENDER", "2").register()
        .build()
        .startAwait();

    Call service = gateway.call().router((reg, msg) -> reg.listServiceReferences().stream().filter(ref -> "2".equals(
        ref.tags().get("SENDER"))).collect(Collectors.toList()));

    // call the service.
    for (int i = 0; i < 1e3; i++) {
      GreetingResponse result =
          Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block().data();
      assertEquals("2", result.sender());
    }
    provider2.shutdown().block();
    provider1.shutdown().block();
    gateway.shutdown().block();
  }

  @Test
  public void test_tag_request_selection_logic() {
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .service(new GreetingServiceImpl(1)).tag("ONLYFOR", "joe").register()
        .build()
        .startAwait();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .service(new GreetingServiceImpl(2)).tag("ONLYFOR", "fransin").register()
        .build()
        .startAwait();

    Call service = gateway.call().router(
        (reg, msg) -> reg.listServiceReferences().stream().filter(ref -> ((GreetingRequest) msg.data()).getName()
            .equals(ref.tags().get("ONLYFOR"))).collect(Collectors.toList()));

    // call the service.
    for (int i = 0; i < 1e3; i++) {
      GreetingResponse resultForFransin =
          Mono.from(service.requestOne(GREETING_REQUEST_REQ2, GreetingResponse.class)).timeout(timeout).block().data();
      GreetingResponse resultForJoe =
          Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block().data();
      assertEquals("1", resultForJoe.sender());
      assertEquals("2", resultForFransin.sender());
    }
    provider2.shutdown().block();
    provider1.shutdown().block();
    gateway.shutdown().block();
  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() throws Exception {
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = provider(gateway);

    Call service = provider1.call();

    CountDownLatch timeLatch = new CountDownLatch(1);
    try {
      // call the service.
      Mono.from(service.requestOne(NOT_FOUND_REQ)).block(timeout);
      fail("Expected no-service-found exception");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("No reachable member with such service: " + NOT_FOUND_REQ.qualifier()));
      timeLatch.countDown();
    }

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    gateway.shutdown().block();
    provider1.shutdown().block();
  }

  @Test
  public void test_service_tags() throws Exception {
    Microservices gateway = gateway();

    Microservices services1 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImplA()).tag("Weight", "0.3").register()
        .build()
        .startAwait();

    Microservices services2 = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImplB()).tag("Weight", "0.7").register()
        .build()
        .startAwait();

    System.out.println(gateway.cluster().members());

    TimeUnit.SECONDS.sleep(3);
    Call service = gateway.call().router(CanaryTestingRouter.class);

    ServiceMessage req = Messages.builder()
        .request(CanaryService.class, "greeting")
        .data(new GreetingRequest("joe"))
        .build();

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e3;
    CountDownLatch timeLatch = new CountDownLatch(n);
    for (int i = 0; i < n; i++) {
      Mono<ServiceMessage> response = service.requestOne(req, GreetingResponse.class);
      response.doOnNext(message -> {
        timeLatch.countDown();
        if (message.data().toString().contains("SERVICE_B_TALKING")) {
          serviceBCount.incrementAndGet();
        }
      }).subscribe();
    }

    timeLatch.await(1, TimeUnit.MINUTES);
    assertEquals(0, timeLatch.getCount());

    System.out.println("count: " + serviceBCount.get());
    System.out.println("Service B was called: " + serviceBCount.get() + " times.");

    assertEquals(0.6d, serviceBCount.doubleValue() / n, 0.2d);

    services1.shutdown().block();
    services2.shutdown().block();
    gateway.shutdown().block();
  }

  @Test
  public void test_dispatcher_remote_greeting_request_completes_before_timeout() {
    // Create microservices instance.
    Microservices gateway = gateway();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    Publisher<ServiceMessage> result = gateway.call().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).block(Duration.ofSeconds(TIMEOUT)).data();
    System.out.println("greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));

    gateway.shutdown().block();
    node.shutdown().block();
  }

  @Test
  public void test_dispatcher_local_greeting_request_completes_before_timeout() {
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    Call service = gateway.call();

    Publisher<ServiceMessage> result = service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class);

    GreetingResponse greetings = Mono.from(result).timeout(Duration.ofSeconds(TIMEOUT)).block().data();
    System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
    assertTrue(greetings.getResult().equals(" hello to: joe"));

    gateway.shutdown().block();
  }

  private Microservices provider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();
  }

  private Microservices gateway() {
    return Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();
  }

  private boolean await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) throws Exception {
    return timeLatch.await(timeout, timeUnit);
  }
}
