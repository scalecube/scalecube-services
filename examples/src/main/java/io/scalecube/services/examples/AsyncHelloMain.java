package io.scalecube.services.examples;


import io.scalecube.services.Microservices;

import java.util.concurrent.CompletableFuture;

public class AsyncHelloMain {


  public static void main(String[] args) throws Exception {

    simpleAsyncInvoke();

    distributedAsyncInvoke();

  }

  private static void simpleAsyncInvoke() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
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
        // print the greeting.
        System.out.println(result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });

    microservices.cluster().shutdown();
  }


  private static void distributedAsyncInvoke() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
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
        System.out.println(result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }

      System.exit(0);
    });
  }


}
