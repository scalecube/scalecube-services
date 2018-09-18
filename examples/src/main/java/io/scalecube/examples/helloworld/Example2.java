package io.scalecube.examples.helloworld;

import io.scalecube.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.examples.helloworld.service.api.Greeting;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.api.ServiceMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * The Hello World project is a time-honored tradition in computer programming. It is a simple
 * exercise that gets you started when learning something new. Let’s get started with ScaleCube!
 *
 * <p>The example starts 2 cluster member nodes. 1. seed is a member node and holds no services of
 * its own. 2. The <code>microservices</code> variable is a member that joins seed member and
 * provision <code>GreetingService</code> instance. This Code demonstrates executing a ScaleCube
 * service using a <code>ServiceMessage</code> rather than an explicit Service interface thus
 * eliminating Service interface dependency.
 */
public class Example2 {

  static final String SERVICE_QUALIFIER = "/io.scalecube.Greetings/sayHello";

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

    // Create a proxy to the seed service node
    Call service = seed.call();

    // Create a ServiceMessage request with service qualifier and data
    ServiceMessage request =
        ServiceMessage.builder().qualifier(SERVICE_QUALIFIER).data("joe").build();
    // Execute the Greeting Service to emit a single Greeting response
    Publisher<ServiceMessage> publisher = service.create().requestOne(request, Greeting.class);

    // Convert the Publisher using the Mono API which ensures it will emit 0 or 1 item.
    Mono.from(publisher)
        .subscribe(
            consumer -> {
              // handle service response
              Greeting greeting = consumer.data();
              System.out.println(greeting.message());
            });

    // shut down the nodes
    seed.shutdown().block();
    microservices.shutdown().block();
  }
}
