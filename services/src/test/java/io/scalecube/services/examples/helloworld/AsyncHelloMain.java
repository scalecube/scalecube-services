package io.scalecube.services.examples.helloworld;


import java.util.concurrent.CompletableFuture;

import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.HelloWorldComponent;

public class AsyncHelloMain {


  public static void main(String[] args) {

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
        // print the greeting.
        System.out.println(result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
    });


  }

}
