package io.scalecube.examples.services;

import io.scalecube.examples.services.TicketServiceImpl.TickerServiceConfig;
import io.scalecube.examples.services.UserServiceImpl.UserServiceConfig;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInjector;
import io.scalecube.transport.Address;

import java.util.concurrent.CompletableFuture;

/**
 * Example of scalecube services.
 */
public class MicroservicesInjectableExample {

  public static void main(String[] args) throws Exception {
    Address providerAddress = onProvider();
    onConsumer(providerAddress);
  }

  private static Address onProvider() {
    //create the injector
    ServiceInjector injector = ServiceInjector.builder()
        .bind(TickerServiceConfig.class)
        .to(new TickerServiceConfig(10)).build();
    // Create microservice provider
    Microservices provider = Microservices.builder()
        .services().from(TicketServiceImpl.class).build().injector(injector)
        .build();

    // Return provider address
    return provider.cluster().address();
  }

  private static void onConsumer(Address providerAddress) throws Exception {
     //create the injector
    ServiceInjector injector = ServiceInjector.builder()
        .bind(UserServiceConfig.class)
        .to(new UserServiceConfig("TestVenue")).build();
    // Create microservice consumer
    Microservices consumer = Microservices.builder().services().from(UserServiceImpl.class).build().injector(injector)
        .seeds(providerAddress).build();

    // Get a proxy to the service API
    UserService userService = consumer.proxy().api(UserService.class).create();

    CompletableFuture<Boolean> ret = userService.reserveTickets(100);
    ret.whenComplete((result, exception) -> System.out.println("ret " + result));

  }

}
