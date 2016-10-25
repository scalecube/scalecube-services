package io.scalecube.services.examples;

import org.consul.registry.integration.ConsulServiceDiscovery;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
    ListenableFuture<GreetingResponse> futureResult = service.asyncGreeting(new GreetingRequest("joe"));

    Futures.addCallback(futureResult, new FutureCallback<GreetingResponse>() {

      @Override
      public void onSuccess(GreetingResponse result) {
        // print the greeting.
        System.out.println("non-blocking call result: " + result.greeting());
      }

      @Override
      public void onFailure(Throwable t) {}
    });
    
    
    // example-2: call the service. (blocking call)
    GreetingResponse result = service.greeting(new GreetingRequest("joe"));

    // print the greeting.
    System.out.println("blocking call result: " +  result.greeting());

  }
}
