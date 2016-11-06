package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ServicesIT {

  private static AtomicInteger port = new AtomicInteger(4000);

  @Test
  public void remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);
    
    // Create microservices instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();
    
     Microservices.builder()
       .seeds(gateway.cluster().address())
       .services(new GreetingServiceImpl())
       .build();
       

     GreetingService service = gateway.proxy()
         .api(GreetingService.class)
         .create();
     
    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe",duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success,error)->{
      if(error==null) {
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + success.getResult());
        assertTrue(success.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      }
    });
    
    await(timeLatch,60,TimeUnit.SECONDS);
  }
  
  @Test
  public void greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);
    
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe",duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success,error)->{
      if(error==null) {
        // print the greeting.
        System.out.println("2. greeting_request_completes_before_timeout : " + success.getResult());
        assertTrue(success.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      }
    });
    
    await(timeLatch,60,TimeUnit.SECONDS);
  }
  
  @Test
  public void local_async_greeting() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<String> future = service.asyncGreeting("joe");

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.equals(" hello to: joe"));
        // print the greeting.
        System.out.println("3. local_async_greeting :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }

  @Test
  public void remote_async_greeting_return_string() {
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
    CompletableFuture<String> future = service.asyncGreeting("joe");

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("4. remote_async_greeting_return_string :" + result);

        assertTrue(result.equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    await(TimeUnit.MILLISECONDS, 5);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void local_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<GreetingResponse> future = service.asyncGreetingRequest(new GreetingRequest("joe"));

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.getResult().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("5. remote_async_greeting_return_GreetingResponse :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }

  @Test
  public void remote_async_greeting_return_GreetingResponse() {
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
    CompletableFuture<GreetingResponse> future = service.asyncGreetingRequest(new GreetingRequest("joe"));

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
    });
    await(TimeUnit.MILLISECONDS, 5);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void local_greeting_request_timeout_expires() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    Duration duration = Duration.ofSeconds(4);
    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe",duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success,error)->{
      if(error!=null)
      // print the greeting.
      System.out.println("7. local_greeting_request_timeout_expires : " + error);
      assertTrue(error instanceof TimeoutException);
      timeLatch.countDown();
    });
    
    await(timeLatch,60,TimeUnit.SECONDS);
  }

  @Test
  public void remote_greeting_request_timeout_expires() {
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

    Duration duration = Duration.ofSeconds(4);
    // call the service.
    CompletableFuture<GreetingResponse> result = service.greetingRequestTimeout(new GreetingRequest("joe",duration));

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success,error)->{
      if(error!=null)
      // print the greeting.
      System.out.println("8. remote_greeting_request_timeout_expires : " + error);
      assertTrue(error instanceof TimeoutException);
      timeLatch.countDown();
    });
    
    await(timeLatch,60,TimeUnit.SECONDS);
  }

  @Test
  public void local_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // get a proxy to the service api.
    GreetingService service = createProxy(microservices);

    // call the service.
    CompletableFuture<Message> future = service.asyncGreetingMessage(Message.builder().data("joe").build());

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.data().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("9. local_async_greeting_return_Message :" + result.data());
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    
    await(TimeUnit.MILLISECONDS, 5);
    microservices.cluster().shutdown();
  }

  @Test
  public void remote_async_greeting_return_Message() {
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
    CompletableFuture<Message> future = service.asyncGreetingMessage(Message.builder().data("joe").build());

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("10. remote_async_greeting_return_Message :" + result.data());
        // print the greeting.
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });
    
    await(TimeUnit.MILLISECONDS, 5);
    consumer.cluster().shutdown();
  }

  @Test
  public void round_robin_selection_logic() {
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

    CompletableFuture<Message> result1 = service.asyncGreetingMessage(Message.builder().data("joe").build());
    CompletableFuture<Message> result2 = service.asyncGreetingMessage(Message.builder().data("joe").build());

    CompletableFuture<Void> combined = CompletableFuture.allOf(result1, result2);
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
    });
    
    provider1.cluster().shutdown();
    provider2.cluster().shutdown();    
    gateway.cluster().shutdown();
  }
  
  @Test
  public void testAsyncGreetingErrorCase() {
    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider1 = createProvider(gateway);
    
    GreetingService service = createProxy(gateway);
    
   CompletableFuture<String> future = service.asyncGreeting("hello");
   future.whenComplete((success,error)->{
     assertTrue(error.getMessage().equals("No reachable member with such service: asyncGreeting"));
   }); 
   gateway.cluster().shutdown();
   provider1.cluster().shutdown();
  }
  
  @Test
  public void testGreetingErrorCase() {
    // Create gateway cluster instance.
    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider1 = createProvider(gateway);
    
    GreetingService service = createProxy(gateway);
   try{
     service.asyncGreeting("hello");
   } catch(Throwable th) {
     assertTrue(th.getCause().getMessage().equals("java.lang.IllegalStateException: No reachable member with such service: greeting"));
   }
   
   gateway.cluster().shutdown();
   provider1.cluster().shutdown();
  }

  private GreetingService createProxy(Microservices gateway) {
    return gateway.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();
  }

  private Microservices createProvider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .build();
  }

  private Microservices createSeed() {
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();
    return gateway;
  }
  
  private void await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) {
    try {
      timeLatch.await(timeout,timeUnit);
    } catch (InterruptedException e) {
    }
  }
  
  private void await(TimeUnit timeunit, int i) {
    try {
      timeunit.sleep(i);
    } catch (InterruptedException e) {
    }
  }

  

}
