package io.scalecube.services.examples.helloworld;


import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.HelloWorldComponent;

public class SyncHelloMain {

  public static void main(String[] args) {

    simpleBlockingCallExample();

    distributedBlockingCallExample();
  }

  private static void simpleBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .services(new HelloWorldComponent())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);
  }

  private static void distributedBlockingCallExample() {
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
    System.out.println(result);

    System.exit(0);
  }


}
