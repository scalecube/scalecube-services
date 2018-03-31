package io.scalecube.examples.services;

import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;

import java.util.concurrent.CompletableFuture;

/**
 * Example of scalecube services.
 */
public class MicroservicesExample {

  public static void main(String[] args) throws Exception {
    Address providerAddress = onProvider();
    onConsumer(providerAddress);
  }

  private static Address onProvider() {
    // Create microservice provider
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    // Return provider address
    return provider.cluster().address();
  }

  private static void onConsumer(Address providerAddress) throws Exception {
    // Create microservice consumer
    Microservices consumer = Microservices.builder().seeds(providerAddress).build();

    // Get a proxy to the service API
    GreetingService greetingService = consumer.call().api(GreetingService.class);

    // Call service (successful case)
    CompletableFuture<String> future = greetingService.greeting("Joe");
    future.whenComplete((result, exception) ->
        System.out.println("Consumer: 'greeting' <- " + (exception == null ? result : exception)));

    // Call service (error case)
    CompletableFuture<String> futureError = greetingService.greetingException("Joe");
    futureError.whenComplete((result, exception) ->
        System.out.println("Consumer: 'greetingException' <- " + (exception == null ? result : exception)));
  }


}
