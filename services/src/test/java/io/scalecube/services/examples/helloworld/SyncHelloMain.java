package io.scalecube.services.examples.helloworld;


import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.HelloWorldComponent;
import io.scalecube.transport.Address;

public class SyncHelloMain {

  public static void main(String[] args) {

    simpleBlockingCallExample();

    distributedBlockingCallExample();
  }

  private static void simpleBlockingCallExample() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .services(new HelloWorldComponent())
        .build();
    
    GreetingService service = microservices.proxy().api(GreetingService.class).create();

    // call the service.
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);
  }

  private static void distributedBlockingCallExample() {
    // Create microservices cluster.
    Microservices.builder()
        .port(4015)
        .services(new HelloWorldComponent())
        .build();

    GreetingService service = Microservices.builder()
        .seeds(Address.create("localhost", 4015)) // join cluster on specific port
        .build()
        .proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);
  }


}
