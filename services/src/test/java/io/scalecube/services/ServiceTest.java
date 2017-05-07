package io.scalecube.services;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.a.b.testing.CanaryService;
import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceTest extends BaseTest {

  private static AtomicInteger port = new AtomicInteger(4000);

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    // Create microservices instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

    Microservices node2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = gateway.proxy()
        .api(GreetingService.class)
        .timeout(Duration.ofSeconds(3))
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe", duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + success.getResult());
        assertTrue(success.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      } else {
        System.out.println("1. FAILED! - greeting_request_completes_before_timeout reached timeout: " + error);
        assertTrue(error.toString(), false);
        timeLatch.countDown();
      }
    });

    await(timeLatch, 10, TimeUnit.SECONDS);

    node2.cluster().shutdown();
    gateway.cluster().shutdown();

  }

  @Test
  public void test_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.proxy().api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe", duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        // print the greeting.
        System.out.println("2. greeting_request_completes_before_timeout : " + success.getResult());
        assertTrue(success.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      }
    });

    await(timeLatch, 60, TimeUnit.SECONDS);
    node1.cluster().shutdown();

  }

  @Test
  public void test_local_async_greeting() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<String> future = service.greeting("joe");

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.equals(" hello to: joe"));
        // print the greeting.
        System.out.println("3. local_async_greeting :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_local_async_no_params() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<String> future = service.greetingNoParams();

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.equals("hello unknown"));
        // print the greeting.
        System.out.println("test_local_async_no_params :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_void_greeting() throws InterruptedException {
    // Create microservices instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = gateway.proxy()
        .api(GreetingService.class)
        .timeout(Duration.ofSeconds(3))
        .create();

    // call the service.
    service.greetingVoid(new GreetingRequest("joe"));

    // send and forget so we have no way to know what happen
    // but at least we didn't get exception :)
    assertTrue(true);
    System.out.println("test_remote_void_greeting done.");

    Thread.sleep(1000);

    gateway.cluster().shutdown();
    node1.cluster().shutdown();
  }

  @Test
  public void test_local_void_greeting() {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    service.greetingVoid(new GreetingRequest("joe"));

    // send and forget so we have no way to know what happen
    // but at least we didn't get exception :)
    assertTrue(true);
    System.out.println("test_local_void_greeting done.");
    node1.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    CompletableFuture<String> future = service.greeting("joe");

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("4. remote_async_greeting_return_string :" + result);
        assertTrue(result.equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    CompletableFuture<String> future = service.greetingNoParams();

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("test_remote_async_greeting_no_params :" + result);
        assertTrue(result.equals("hello unknown"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_local_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.getResult().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("5. remote_async_greeting_return_GreetingResponse :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    CompletableFuture<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("6. remote_async_greeting_return_GreetingResponse :" + result.getResult());
        // print the greeting.
        assertTrue(result.getResult().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.proxy().api(GreetingService.class)
        .timeout(Duration.ofSeconds(1))
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> result =
        service.greetingRequestTimeout(new GreetingRequest("joe", Duration.ofSeconds(2)));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error != null) {
        // print the greeting.
        System.out.println("7. local_greeting_request_timeout_expires : " + error);
        assertTrue(error instanceof TimeoutException);
      } else {
        assertTrue("7. failed", false);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 5, TimeUnit.SECONDS);
    node1.cluster().shutdown();
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer, Duration.ofSeconds(1));

    // call the service.
    CompletableFuture<GreetingResponse> result =
        service.greetingRequestTimeout(new GreetingRequest("joe", Duration.ofSeconds(4)));

    CountDownLatch timeLatch = new CountDownLatch(1);

    result.whenComplete((success, error) -> {
      if (error != null) {
        // print the greeting.
        System.out.println("8. remote_greeting_request_timeout_expires : " + error);
        assertTrue(error instanceof TimeoutException);
        timeLatch.countDown();
      }
    });

    try {
      await(timeLatch, 10, TimeUnit.SECONDS);
    } catch (Exception ex) {
      fail();
    }
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_local_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<Message> future = service.greetingMessage(Message.builder().data("joe").build());

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
    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    CompletableFuture<Message> future = service.greetingMessage(Message.builder().data("joe").build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("10. remote_async_greeting_return_Message :" + result.data());
        // print the greeting.
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println("10 failed: " + ex);
        assertTrue(result.data().equals(" hello to: joe"));
      }
      timeLatch.countDown();
    });

    await(timeLatch, 20, TimeUnit.SECONDS);
    consumer.cluster().shutdown();
  }

  @Test
  public void test_round_robin_selection_logic() {
    Microservices gateway = createSeed();

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

    GreetingService service = createProxy(gateway);

    CompletableFuture<Message> result1 = service.greetingMessage(Message.builder().data("joe").build());
    CompletableFuture<Message> result2 = service.greetingMessage(Message.builder().data("joe").build());

    CompletableFuture<Void> combined = CompletableFuture.allOf(result1, result2);
    CountDownLatch timeLatch = new CountDownLatch(1);
    combined.whenComplete((v, x) -> {
      try {
        // print the greeting.
        System.out.println("11. round_robin_selection_logic :" + result1.get());
        System.out.println("11. round_robin_selection_logic :" + result2.get());

        boolean success = !result1.get().sender().equals(result2.get().sender());

        assertTrue(success);
      } catch (Throwable e) {
        assertTrue(false);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 2, TimeUnit.SECONDS);

    provider2.cluster().shutdown();
    provider1.cluster().shutdown();

  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() {
    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider1 = createProvider(gateway);

    GreetingService service = createProxy(gateway);
    CountDownLatch timeLatch = new CountDownLatch(1);
    try {
      service.greeting("hello");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("No reachable member with such service: greeting"));
      timeLatch.countDown();
    }

    await(timeLatch, 1, TimeUnit.SECONDS);
    gateway.cluster().shutdown();
    provider1.cluster().shutdown();
  }

  @Test
  public void test_naive_stress_not_breaking_the_system() throws InterruptedException {
    // Create microservices cluster member.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster member.
    Microservices provider2 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .seeds(provider.cluster().address())
        .build();
    
    // Create microservices cluster member.
    Microservices provider3 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .seeds(provider.cluster().address())
        .build();
    
    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    // Get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // Init params
    int warmUpCount = 1_000;
    int count = 100_000;
    CountDownLatch warmUpLatch = new CountDownLatch(warmUpCount);

    // Warm up
    for (int i = 0; i < warmUpCount; i++) {
      CompletableFuture<Message> future = service.greetingMessage(Message.fromData("naive_stress_test"));
      future.whenComplete((success, error) -> {
        if (error == null) {
          warmUpLatch.countDown();
        }
      });
    }
    warmUpLatch.await(30, TimeUnit.SECONDS);
    assertTrue(warmUpLatch.getCount() == 0);

    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      CompletableFuture<Message> future = service.greetingMessage(Message.fromData("naive_stress_test"));
      future.whenComplete((success, error) -> {
        if (error == null) {
          countLatch.countDown();
        }
      });
    }
    System.out.println("Finished sending " + count + " messages in " + (System.currentTimeMillis() - startTime));
    countLatch.await(60, TimeUnit.SECONDS);
    System.out.println("Finished receiving " + count + " messages in " + (System.currentTimeMillis() - startTime));
    assertTrue(countLatch.getCount() == 0);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_serviceA_calls_serviceB_using_setter() throws InterruptedException {

    Microservices gateway = createSeed();

    CoarseGrainedServiceImpl coarseGrained = new CoarseGrainedServiceImpl();

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(greeting, coarseGrained) // add service a and b
        .build();

    // Get Proxy to greeting service.
    GreetingService proxy = gateway.proxy().api(GreetingService.class).create();

    // provide the proxy of service B
    coarseGrained.setGreetingServiceProxy(proxy);;

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.proxy().api(CoarseGrainedService.class).create();
    CountDownLatch countLatch = new CountDownLatch(1);
    CompletableFuture<String> future = service.callGreeting("joe");
    future.whenComplete((success, error) -> {
      if (error == null) {
        assertTrue(success.equals(" hello to: joe"));
        countLatch.countDown();
      }
    });

    countLatch.await(5, TimeUnit.SECONDS);
    assertTrue(countLatch.getCount() == 0);
    gateway.cluster().shutdown();
    provider.cluster().shutdown();

  }

  @Test
  public void test_serviceA_calls_serviceB_using_constractor() throws InterruptedException {

    Microservices gateway = createSeed();

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl(gateway.proxy().api(GreetingService.class).create());

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(greeting, another) // add service a and b
        .build();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.proxy().api(CoarseGrainedService.class).create();
    CountDownLatch countLatch = new CountDownLatch(1);
    CompletableFuture<String> future = service.callGreeting("joe");
    future.whenComplete((success, error) -> {
      if (error == null) {
        assertTrue(success.equals(" hello to: joe"));
        countLatch.countDown();
      }
    });

    countLatch.await(5, TimeUnit.SECONDS);
    assertTrue(countLatch.getCount() == 0);
    gateway.cluster().shutdown();
    provider.cluster().shutdown();

  }

  @Ignore
  @Test
  public void test_service_tags() {
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

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

    CanaryService service = gateway.proxy()
        .router(CanaryTestingRouter.class)
        .api(CanaryService.class).create();

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger responses = new AtomicInteger(0);
    CountDownLatch timeLatch = new CountDownLatch(1);
    for (int i = 0; i < 100; i++) {
      service.greeting("joe").whenComplete((success, error) -> {
        responses.incrementAndGet();
        if (success.startsWith("B")) {
          count.incrementAndGet();
          if ((responses.get() == 100) && (60 < count.get() && count.get() < 80)) {
            timeLatch.countDown();
          }
        }
      });
    }

    await(timeLatch, 1, TimeUnit.SECONDS);
    assertTrue((responses.get() == 100) && (60 < count.get() && count.get() < 80));
    
    gateway.cluster().shutdown();
    services1.cluster().shutdown();
    services2.cluster().shutdown();
  }

  private GreetingService createProxy(Microservices gateway) {
    return gateway.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();
  }

  private GreetingService createProxy(Microservices micro, Duration duration) {
    return micro.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .timeout(duration)
        .create();
  }

  private Microservices createProvider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .build();
  }

  private Microservices createSeed() {
    return Microservices.builder()
        .port(port.incrementAndGet())
        .build();
  }

  private void await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) {
    try {
      timeLatch.await(timeout, timeUnit);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }
}
