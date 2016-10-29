package io.scalecube.services.examples;


import io.scalecube.services.Microservices;

public class HelloMain {


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
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);

  }

}
