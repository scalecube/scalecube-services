package io.scalecube.services.examples;

import io.scalecube.examples.util.Util;
import io.scalecube.services.Microservices;

public class ServiceProvisioningExample {

  public static void main(String[] args) throws InterruptedException {
    // Create microservices cluster instance on port 4040.
    Microservices microservices = Microservices.builder()
        .port(4040)
        .services(new HelloWorldComponent())
        .build();
 
    Util.sleepForever();
  }
}
