package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

import org.consul.registry.integration.ConsulServiceDiscovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.services.Microservices;
import io.scalecube.services.routing.RoundRubinServiceRouter;
import io.scalecube.transport.Address;

public class ServiceConsumerExample {

  public static void main(String[] args) throws InterruptedException {

    // Create and configure microservices instance.
    Microservices microservices = Microservices.builder()
        .cluster(Cluster.joinAwait(Address.from("localhost:4040")))
        .discovery(new ConsulServiceDiscovery())
        .build();

    // Create service proxy provided service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .router(RoundRubinServiceRouter.class)
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
