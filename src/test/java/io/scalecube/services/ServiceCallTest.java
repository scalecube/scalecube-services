package io.scalecube.services;

import static io.scalecube.services.TestRequests.GREETING_NO_PARAMS_REQUEST;
import static io.scalecube.services.TestRequests.GREETING_VOID_REQ;
import static io.scalecube.services.TestRequests.SERVICE_NAME;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.a.b.testing.CanaryService;
import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.streams.StreamMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceCallTest extends BaseTest {

  public static final int TIMEOUT = 3;

  public static final StreamMessage GREETING_REQ = Messages.builder()
      .request(SERVICE_NAME, "greeting")
      .data("joe")
      .build();

  public static final StreamMessage GREETING_REQUEST_REQ = Messages.builder()
      .request(SERVICE_NAME, "greetingRequest")
      .data(new GreetingRequest("joe"))
      .build();

  public static final StreamMessage GREETING_REQUEST_TIMEOUT_REQ = Messages.builder()
      .request(SERVICE_NAME, "greetingRequestTimeout")
      .data(new GreetingRequest("joe", Duration.ofSeconds(3)))
      .build();

  // public static final StreamMessage GREETING_MESSAGE_REQ = Messages.builder()
  // .request(SERVICE_NAME, "greetingMessage")
  // .data("joe").build();

  public static final StreamMessage NOT_FOUND_REQ = Messages.builder()
      .request(SERVICE_NAME, "unknown")
      .data("joe").build();

  private static AtomicInteger port = new AtomicInteger(4000);


  @Test
  public void test_local_async_no_params() throws Exception {
    // Create microservices cluster.
    Microservices microservices = serviceProvider();

    Router router = microservices.router(RoundRobinServiceRouter.class);
    Call serviceCall = ServiceCall.call().router(router);

    // call the service.
    CompletableFuture<StreamMessage> future =
        serviceCall.responseTypeOf(GreetingResponse.class).invoke(GREETING_NO_PARAMS_REQUEST);

    future.whenComplete((message, ex) -> {
      if (ex == null) {
        assertEquals("Didn't get desired response", GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier());
      } else {
        fail("Failed to invoke service: " + ex.getMessage());
      }
    }).get(TIMEOUT, TimeUnit.SECONDS);
    microservices.shutdown().get();
  }

  private Microservices serviceProvider() {
    return Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();
  }

  @Test
  public void test_remote_async_greeting_no_params() throws Exception {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    Call serviceCall = consumer.call();

    // call the service.
    CompletableFuture<StreamMessage> future =
        serviceCall.responseTypeOf(GreetingResponse.class).invoke(GREETING_NO_PARAMS_REQUEST);

    future.whenComplete((StreamMessage message, Throwable ex) -> {
      if (ex == null) {
        assertEquals("Didn't get desired response", GREETING_NO_PARAMS_REQUEST.qualifier(), message.qualifier());
        assertThat(message.data(), instanceOf(GreetingResponse.class));
        assertTrue(((GreetingResponse) message.data()).getResult().equals("hello unknown"));
      } else {
        fail("Failed to invoke service: " + ex.getMessage());
      }
    }).get(TIMEOUT, TimeUnit.SECONDS);
    provider.shutdown().get();
    consumer.shutdown().get();
  }

  @Test
  public void test_remote_void_greeting() throws InterruptedException, ExecutionException, TimeoutException {
    // Given
    Microservices gateway = gateway();

    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    // When
    CompletableFuture<StreamMessage> resultFuture =
        gateway.call().responseTypeOf(GreetingResponse.class).invoke(GREETING_VOID_REQ);
    StreamMessage result = resultFuture.get(TIMEOUT, TimeUnit.SECONDS);

    // Then:
    assertNotNull(result);
    assertEquals(GREETING_VOID_REQ.qualifier(), result.qualifier());
    assertNull(result.data());

    gateway.shutdown().get();
    node1.shutdown().get();
  }

  @Test
  public void test_local_void_greeting() throws Exception {
    // Create microservices instance.
    Microservices node = serviceProvider();
    // call the service.
    CompletableFuture<StreamMessage> future =
        node.call().responseTypeOf(GreetingResponse.class).invoke(GREETING_VOID_REQ);
    StreamMessage result = future.get(TIMEOUT, TimeUnit.SECONDS);
    assertNotNull(result);
    assertEquals(GREETING_VOID_REQ.qualifier(), result.qualifier());
    assertNull(result.data());

    TimeUnit.SECONDS.sleep(2);
    node.shutdown().get();
  }

  @Test
  public void test_remote_async_greeting_return_string()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    CompletableFuture<StreamMessage> resultFuture = consumer.call().responseTypeOf(String.class).invoke(GREETING_REQ);

    // Then
    StreamMessage result = resultFuture.get(TIMEOUT, TimeUnit.SECONDS);
    assertNotNull(result);
    assertEquals(GREETING_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", result.data());

    provider.shutdown().get();
    consumer.shutdown().get();
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Given
    Microservices microservices = serviceProvider();

    // When
    CompletableFuture<StreamMessage> resultFuture =
        microservices.call().responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);

    // Then
    StreamMessage result = resultFuture.get(TIMEOUT, TimeUnit.SECONDS);
    assertNotNull(result);
    assertEquals(GREETING_REQUEST_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", ((GreetingResponse) result.data()).getResult());

    microservices.shutdown().get();
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Given
    Microservices provider = serviceProvider();
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // When
    CompletableFuture<StreamMessage> resultFuture =
        consumer.call().responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);

    // Then
    StreamMessage result = resultFuture.get(TIMEOUT, TimeUnit.SECONDS);
    assertNotNull(result);
    assertEquals(GREETING_REQUEST_REQ.qualifier(), result.qualifier());
    assertEquals(" hello to: joe", ((GreetingResponse) result.data()).getResult());

    provider.shutdown().get();
    consumer.shutdown().get();
  }

  @Test(expected = TimeoutException.class)
  public void test_local_greeting_request_timeout_expires()
      throws Throwable {
    // Given:
    Microservices node = serviceProvider();
    Call service = node.call()
        .timeout(Duration.ofSeconds(1));

    // call the service.
    CompletableFuture<StreamMessage> future =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_TIMEOUT_REQ);

    try {
      future.get(TIMEOUT, TimeUnit.SECONDS);
    } catch (ExecutionException ex) {
      throw ex.getCause();
    } finally {
      node.shutdown().get();
    }

  }

  @Test
  public void test_remote_greeting_request_timeout_expires() throws Throwable {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    Call service = consumer.call()
        .timeout(Duration.ofSeconds(1));

    // call the service.
    CompletableFuture<StreamMessage> future =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_TIMEOUT_REQ);
    try {
      future.get(TIMEOUT, TimeUnit.SECONDS);
    } catch (ExecutionException ex) {
      Assert.assertEquals("io.scalecube.streams.onError/500", ex.getCause().getMessage());
    } finally {
      provider.shutdown().get();
      consumer.shutdown().get();
    }
  }

  // Since here and below tests were not reviewed [sergeyr]

  @Test
  public void test_local_async_greeting_return_Message() throws InterruptedException, ExecutionException {
    // Given:
    Microservices microservices = serviceProvider();


    Call service = microservices.call()
        .timeout(Duration.ofSeconds(1));

    // call the service.
    CompletableFuture<StreamMessage> future =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);


    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.data().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("9. local_async_greeting_return_Message :" + result.data());
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    TimeUnit.SECONDS.sleep(1);
    microservices.shutdown().get();
  }

  @Test
  public void test_remote_async_greeting_return_Message() throws Exception {
    // Create microservices cluster.
    Microservices provider = serviceProvider();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    Call service = consumer.call();

    // call the service.
    CompletableFuture<StreamMessage> future =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("10. remote_async_greeting_return_Message :" + result.data());
        // print the greeting.
        assertThat(result.data(), instanceOf(GreetingResponse.class));
        assertTrue(((GreetingResponse) result.data()).getResult().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println("10 failed: " + ex);
        assertTrue(result.data().equals(" hello to: joe"));
      }
      timeLatch.countDown();
    });

    assertTrue(await(timeLatch, 3, TimeUnit.SECONDS));
    consumer.shutdown().get();
    provider.shutdown().get();
  }

  @Test
  public void test_round_robin_selection_logic() throws Exception {
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    Call service = gateway.call();

    // call the service.
    CompletableFuture<StreamMessage> result1 =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);
    CompletableFuture<StreamMessage> result2 =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);

    CompletableFuture<Void> combined = CompletableFuture.allOf(result1, result2);
    CountDownLatch timeLatch = new CountDownLatch(1);
    final AtomicBoolean success = new AtomicBoolean(false);
    combined.whenComplete((v, x) -> {
      try {
        // print the greeting.
        System.out.println("11. round_robin_selection_logic :" + result1.get());
        System.out.println("11. round_robin_selection_logic :" + result2.get());

        GreetingResponse response1 = result1.get().data();
        GreetingResponse response2 = result2.get().data();

        success.set(!response1.sender().equals(response2.sender()));
      } catch (Throwable e) {
        assertTrue(false);
      }
      timeLatch.countDown();
    });

    assertTrue(await(timeLatch, 2, TimeUnit.SECONDS));
    assertTrue(timeLatch.getCount() == 0);
    assertTrue(success.get());
    provider2.shutdown().get();
    provider1.shutdown().get();
    gateway.shutdown().get();
  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case()
      throws Exception {
    Microservices gateway = gateway();

    // Create microservices instance cluster.
    Microservices provider1 = provider(gateway);

    Call service = provider1.call();

    CountDownLatch timeLatch = new CountDownLatch(1);
    try {
      // call the service.
      CompletableFuture<StreamMessage> future = service.invoke(NOT_FOUND_REQ);

    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("No reachable member with such service: " + NOT_FOUND_REQ.qualifier()));
      timeLatch.countDown();
    }

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    gateway.shutdown().get();
    provider1.shutdown().get();
  }

  @Ignore("https://api.travis-ci.org/v3/job/346827972/log.txt")
  @Test
  public void test_service_tags() throws Exception {
    Microservices gateway = gateway();

    Microservices services1 = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplA()).tag("Weight", "0.3").add()
        .build()
        .build();

    Microservices services2 = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplB()).tag("Weight", "0.7").add()
        .build()
        .build();

    System.out.println(gateway.cluster().members());

    TimeUnit.SECONDS.sleep(3);
    Call service = gateway.call()
        .router(gateway.router(CanaryTestingRouter.class));

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger responses = new AtomicInteger(0);
    CountDownLatch timeLatch = new CountDownLatch(1);

    for (int i = 0; i < 100; i++) {
      // call the service.
      CompletableFuture<StreamMessage> future = service.invoke(Messages.builder()
          .request(CanaryService.class, "greeting")
          .data("joe")
          .build());

      future.whenComplete((success, error) -> {
        responses.incrementAndGet();
        if (success.data().toString().startsWith("B")) {
          count.incrementAndGet();
          if ((responses.get() == 100) && (60 < count.get() && count.get() < 80)) {
            timeLatch.countDown();
          }
        }
      });
    }

    assertTrue(await(timeLatch, 5, TimeUnit.SECONDS));
    System.out.println("responses: " + responses.get());
    System.out.println("count: " + count.get());
    System.out.println("Service B was called: " + count.get() + " times.");

    assertTrue((responses.get() == 100) && (60 < count.get() && count.get() < 80));
    services1.shutdown().get();
    services2.shutdown().get();
    gateway.shutdown().get();

  }

  @Test
  public void test_dispatcher_remote_greeting_request_completes_before_timeout() throws Exception {

    // Create microservices instance.
    Microservices gateway = gateway();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    Call service = gateway.call().responseTypeOf(GreetingResponse.class).timeout(Duration.ofSeconds(3));

    CompletableFuture<StreamMessage> result = service.invoke(GREETING_REQUEST_REQ);

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println(success);
        GreetingResponse greetings = success.data();
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
        assertTrue(greetings.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      } else {
        System.out.println("1. FAILED! - greeting_request_completes_before_timeout reached timeout: " + error);
        assertTrue(error.toString(), false);
        timeLatch.countDown();
      }
    });

    assertTrue(await(timeLatch, 10, TimeUnit.SECONDS));
    assertTrue(timeLatch.getCount() == 0);
    gateway.shutdown().get();
    node.shutdown().get();
  }

  @Test
  public void test_dispatcher_local_greeting_request_completes_before_timeout()
      throws Exception {

    Microservices gateway = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    Call service = gateway.call().timeout(Duration.ofSeconds(3));

    CompletableFuture<StreamMessage> result =
        service.responseTypeOf(GreetingResponse.class).invoke(GREETING_REQUEST_REQ);

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println(success);
        GreetingResponse greetings = success.data();
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
        assertTrue(greetings.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      } else {
        System.out.println("1. FAILED! - greeting_request_completes_before_timeout reached timeout: " + error);
        assertTrue(error.toString(), false);
        timeLatch.countDown();
      }
    });

    assertTrue(await(timeLatch, 5, TimeUnit.SECONDS));
    assertTrue(timeLatch.getCount() == 0);
    gateway.shutdown().get();
  }

  private Microservices provider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .build();
  }

  private Microservices gateway() {
    return Microservices.builder()
        .port(port.incrementAndGet())
        .build();
  }

  private boolean await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) throws Exception {
    return timeLatch.await(timeout, timeUnit);
  }
}
