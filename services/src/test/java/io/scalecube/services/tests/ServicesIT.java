package io.scalecube.services.tests;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingResponse;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.HelloWorldComponent;
import io.scalecube.transport.Message;

public class ServicesIT {

  /**
   * NATIVE TESTING
   */
  @Test
  public void simpleAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .services(new HelloWorldComponent())
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
        System.out.println("simpleAsyncInvoke :" +  result);
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
        .services(new HelloWorldComponent())
        .build();
    
    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
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
        assertTrue(result.equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
  }
  
  @Test
  public void simpleSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .services(new HelloWorldComponent())
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
        .services(new HelloWorldComponent())
        .build();

    GreetingService service = Microservices.builder()
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    String result = service.greeting("joe");

    // print the greeting.
    System.out.println("distributedSyncBlockingCallExample :" + result);

    assertTrue(result.equals(" hello to: joe"));

  }
  
  
  /**
   * POJO TESTING
   */
  @Test
  public void pojoAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .services(new HelloWorldComponent())
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
        System.out.println("simpleAsyncInvoke :" +  result);
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
        .services(new HelloWorldComponent())
        .build();
    
    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
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
        assertTrue(result.result().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
  }
  
  @Test
  public void pojoSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .services(new HelloWorldComponent())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    GreetingResponse result = service.greetingRequest(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("simpleSyncBlockingCallExample :" + result.result());
    
    assertTrue(result.result().equals(" hello to: joe"));
  }

  @Test
  public void pojoDistributedSyncBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new HelloWorldComponent())
        .build();

    GreetingService service = Microservices.builder()
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    GreetingResponse result = service.greetingRequest(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("distributedSyncBlockingCallExample :" + result.result());

    assertTrue(result.result().equals(" hello to: joe"));

  }
  
  
  
  
  
  /**
   * MESSAGE TESTING
   */
  @Test
  public void messageAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .services(new HelloWorldComponent())
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
        System.out.println("simpleAsyncInvoke :" +  result);
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
        .services(new HelloWorldComponent())
        .build();
    
    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
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
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
  }
  
  @Test
  public void messageSyncBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .services(new HelloWorldComponent())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    Message result = service.greetingMessage(Message.builder().data("joe").build());

    // print the greeting.
    System.out.println("simpleSyncBlockingCallExample :" + result.data());
    
    assertTrue(result.data().equals(" hello to: joe"));
  }

  @Test
  public void messageDistributedSyncBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new HelloWorldComponent())
        .build();

    GreetingService service = Microservices.builder()
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    Message result = service.greetingMessage(Message.builder().data("joe").build());

    // print the greeting.
    System.out.println("distributedSyncBlockingCallExample :" + result.data());

    assertTrue(result.data().equals(" hello to: joe"));

  }
}