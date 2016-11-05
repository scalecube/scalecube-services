package io.scalecube.examples.services;

import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;

import java.util.concurrent.CompletableFuture;

/**
 * Example of scalecube services.
 */
public class MicroservicesExample {

  private static Microservices provider;
  private static Microservices consumer;

  public static void main(String[] args) throws Exception {
    Address providerAddress = onProvider();
    onConsumer(providerAddress);
  }

  private static Address onProvider() {
    // Create microservice provider
    provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    // Return provider address
    return provider.cluster().address();
  }

  private static void onConsumer(Address providerAddress) throws Exception {
    // Create microservice consumer
    consumer = Microservices.builder().seeds(providerAddress).build();

    // Get a proxy to the service API
    GreetingService greetingService = consumer.proxy().api(GreetingService.class).create();

    // Call service asynchronously
    CompletableFuture<String> future = greetingService.asyncGreeting("Joe");
    future.whenComplete((result, ex) -> System.out.println("Response async: " + (ex == null ? result : ex)));

    // Call service asynchronously with error
    CompletableFuture<String> futureError = greetingService.asyncGreetingException("Joe");
    futureError.whenComplete((result, ex) -> System.out.println("Response async: " + (ex == null ? result : ex)));

    // Call service synchronously (blocking)
    try {
      String result = greetingService.syncGreeting("joe");
      System.out.println("Response sync: " + result);
    } catch (Exception ex) {
      System.out.println("Response sync: " + ex.toString());
    }

    // Call service synchronously (blocking) with error
    try {
      String result = greetingService.syncGreetingException("joe");
      System.out.println("Response sync: " + result);
    } catch (Exception ex) {
      System.out.println("Response sync: " + ex.toString());
    }
  }


}
