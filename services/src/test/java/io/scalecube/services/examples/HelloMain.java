package io.scalecube.services.examples;


import io.scalecube.cluster.Cluster;
import io.scalecube.services.Microservices;

public class HelloMain {


  public static void main(String[] args) {

    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .cluster(Cluster.joinAwait())
        .build();

    // introduce a service instance by registering it.
    microservices.registry()
        .service(new HelloWorldComponent())
        .register();

    // get a proxy to the service api.
    GreetingService service = microservices.proxy()
        .api(GreetingService.class)
        .create();

    // call the service.
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);

  }

}
