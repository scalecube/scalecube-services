package io.scalecube.services.tests;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingResponse;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.transport.Message;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ServicesIT {

  private static AtomicInteger port = new AtomicInteger(4000);

  /**
   * NATIVE TESTING
   */
  @Test
  public void simpleAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<String> future = service.asyncGreeting("joe");

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.equals(" hello to: joe"));
        // print the greeting.
        System.out.println("simpleAsyncInvoke :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }

  @Test
  public void distributedAsyncInvoke() {
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
    GreetingService service = consumer.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<String> future = service.asyncGreeting("joe");

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("distributedAsyncInvoke :" + result);

        assertTrue(result.equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void simpleSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println("simpleSyncBlockingCallExample :" + result);

    assertTrue(result.equals(" hello to: joe"));
  }

  @Test
  public void distributedSyncBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    String result = service.greeting("joe");

    // print the greeting.
    System.out.println("distributedSyncBlockingCallExample :" + result);

    assertTrue(result.equals(" hello to: joe"));

    provider.cluster().shutdown();
  }


  /**
   * POJO TESTING
   */
  @Test
  public void pojoAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> future = service.asyncGreetingRequest(new GreetingRequest("joe"));

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.result().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("pojoAsyncInvoke :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }

  @Test
  public void pojoDistributedAsyncInvoke() {
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
    GreetingService service = consumer.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> future = service.asyncGreetingRequest(new GreetingRequest("joe"));

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("pojoDistributedAsyncInvoke :" + result.result());
        // print the greeting.
        assertTrue(result.result().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void pojoSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    GreetingResponse result = service.greetingRequest(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("pojoSyncBlockingCallExample :" + result.result());

    assertTrue(result.result().equals(" hello to: joe"));
  }

  @Test
  public void pojoDistributedSyncBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    GreetingResponse result = service.greetingRequest(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("pojoDistributedSyncBlockingCallExample :" + result.result());

    assertTrue(result.result().equals(" hello to: joe"));

    provider.cluster().shutdown();
  }



  /**
   * MESSAGE TESTING
   */
  @Test
  public void messageAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<Message> future = service.asyncGreetingMessage(Message.builder().data("joe").build());

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.data().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("messageAsyncInvoke :" + result.data());
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }

  @Test
  public void messageDistributedAsyncInvoke() {
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
    GreetingService service = consumer.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<Message> future = service.asyncGreetingMessage(Message.builder().data("joe").build());

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("messageDistributedAsyncInvoke :" + result.data());
        // print the greeting.
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    consumer.cluster().shutdown();
  }

  @Test
  public void messageSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    Message result = service.greetingMessage(Message.builder().data("joe").build());

    // print the greeting.
    System.out.println("messageSyncBlockingCallExample :" + result.data());

    assertTrue(result.data().equals(" hello to: joe"));
  }

  @Test
  public void messageDistributedSyncBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    Message result = service.greetingMessage(Message.builder().data("joe").build());

    // print the greeting.
    System.out.println("messageDistributedSyncBlockingCallExample :" + result.data());

    assertTrue(result.data().equals(" hello to: joe"));

  }

  @Test
  public void testRoundRubinLogic() {
    // Create gateway cluster instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

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

    GreetingService service = gateway.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    CompletableFuture<Message> result1 = service.asyncGreetingMessage(Message.builder().data("joe").build());
    CompletableFuture<Message> result2 = service.asyncGreetingMessage(Message.builder().data("joe").build());

    CompletableFuture<Void> combined = CompletableFuture.allOf(result1, result2);
    combined.whenComplete((v, x) -> {
      try {
        // print the greeting.
        System.out.println("messageDistributedSyncBlockingCallExample :" + result1.get());
        System.out.println("messageDistributedSyncBlockingCallExample :" + result2.get());
 
        boolean success = !result1.get().sender().equals(result2.get().sender());
        assertTrue(success);
      } catch (Throwable e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    });
  }

}
