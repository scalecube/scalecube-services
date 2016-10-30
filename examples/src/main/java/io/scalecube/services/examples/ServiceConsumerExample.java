package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;

public class ServiceConsumerExample {

  public static void main(String[] args) throws InterruptedException {

    // Create and configure microservices instance.
    Microservices microservices = Microservices.builder()
        .seeds(Address.from("localhost:4040"))
        .build();

    // Create service proxy provided service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .create();
    
    // example-1:  call the service. (non-blocking call)
    CompletableFuture<GreetingResponse> futureResult = service.asyncGreeting(new GreetingRequest("joe"));

    futureResult.whenComplete((success,error)->{
      if(error==null){
        // print the greeting.
        System.out.println("non-blocking call result: " + success.greeting());
      } else {
        
      }   
    });
    
    
    // example-2: call the service. (blocking call)
    GreetingResponse result = service.greeting(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("blocking call result: " +  result.greeting());

  }
}
