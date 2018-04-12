package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterConfig.Builder;
import io.scalecube.services.a.b.testing.CanaryService;
import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LocalServiceTest extends BaseTest {

  private static AtomicInteger port = new AtomicInteger(4000);

 
 
  @Test
  public void test_local_greeting_request_completes_before_timeout() throws Exception {
    Duration duration = Duration.ofSeconds(1);

    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.call().api(GreetingService.class);

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

    assertTrue(await(timeLatch, 60, TimeUnit.SECONDS));
    assertTrue(timeLatch.getCount() == 0);
    node1.shutdown();

  }

  @Test
  public void test_local_async_greeting() throws Exception {
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

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    microservices.shutdown().get();
  }

  @Test
  public void test_local_async_no_params() throws Exception {
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

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    microservices.shutdown().get();
  }

  @Test
  public void test_local_void_greeting() {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.call().api(GreetingService.class);

    // call the service.
    service.greetingVoid(new GreetingRequest("joe"));

    // send and forget so we have no way to know what happen
    // but at least we didn't get exception :)
    assertTrue(true);
    System.out.println("test_local_void_greeting done.");
    node1.shutdown();
  }

  
  
  @Test
  public void test_local_async_greeting_return_GreetingResponse() throws Exception {
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

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    microservices.shutdown().get();
  }

  
  @Test
  public void test_local_greeting_request_timeout_expires() throws Exception {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = node1.call()
        .timeout(Duration.ofSeconds(1))
        .api(GreetingService.class);

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

    assertTrue(await(timeLatch, 5, TimeUnit.SECONDS));
    node1.shutdown().get();
  }

  
  @Test
  public void test_local_async_greeting_return_Message() throws Exception {
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
    future.whenComplete((GreetingResponse result, Throwable ex) -> {
      if (ex == null) {
        assertTrue(result.getResult().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("9. local_async_greeting_return_Message :" + result);
        timeLatch.countDown();
      } else {
        // print the greeting.
        System.out.println(ex);
      } ;
    });

    assertTrue(await(timeLatch, 1, TimeUnit.SECONDS));
    assertTrue(timeLatch.getCount() == 0);
    microservices.shutdown().get();
  }
  
  private GreetingService createProxy(Microservices gateway) {
    return gateway.call()
        .timeout(Duration.ofSeconds(30))
        .api(GreetingService.class); // create proxy for GreetingService API

  }

  private boolean await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) throws Exception {
    return timeLatch.await(timeout, timeUnit);
  }
}
