package io.scalecube.services.examples.helloworld;

import io.scalecube.services.Microservices;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;

/**
 * The Hello World project is a time-honored tradition in computer programming. It is a simple
 * exercise that gets you started when learning something new. Let’s get started with ScaleCube!
 *
 * <p>the example starts 2 cluster member nodes. 1. seed is a member node and holds no services of
 * its own. 2. The <code>microservices</code> variable is a member that joins seed member and
 * provision <code>GreetingService</code> instance.
 */
public class Example1 {

  /**
   * Start the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // ScaleCube Node node with no members
    Microservices seed = Microservices.builder().startAwait();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices microservices =
        Microservices.builder()
            .discovery(options -> options.seeds(seed.discovery().address()))
            .services(new GreetingServiceImpl())
            .startAwait();

    // Create service proxy
    GreetingsService service = seed.call().create().api(GreetingsService.class);

    // Execute the services and subscribe to service events
    service
        .sayHello("joe")
        .subscribe(
            consumer -> {
              System.out.println(consumer.message());
            });

    // shut down the nodes
    seed.shutdown().block();
    microservices.shutdown().block();
  }
}
