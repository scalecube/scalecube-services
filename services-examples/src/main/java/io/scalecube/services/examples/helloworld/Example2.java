package io.scalecube.services.examples.helloworld;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ClusterAddresses;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.examples.ServiceTransports;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.Greeting;
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
   * @throws InterruptedException joining main.
   */
  public static void main(String[] args) throws InterruptedException {
    // ScaleCube Node node with no members
    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .startAwait();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices.builder()
        .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seed))
        .transport(ServiceTransports::rsocketServiceTransport)
        .services(new GreetingServiceImpl())
        .startAwait();

    // Create a proxy to the seed service node
    ServiceCall service = seed.call();

    // Create a ServiceMessage request with service qualifier and data
    ServiceMessage request =
        ServiceMessage.builder().qualifier(SERVICE_QUALIFIER).data("joe").build();
    // Execute the Greeting Service to emit a single Greeting response
    Publisher<ServiceMessage> publisher = service.requestOne(request, Greeting.class);

    // Convert the Publisher using the Mono API which ensures it will emit 0 or 1 item.
    Mono.from(publisher)
        .subscribe(
            consumer -> {
              // handle service response
              Greeting greeting = consumer.data();
              System.out.println(greeting.message());
            });

    Thread.currentThread().join();
  }

  private static ServiceDiscovery serviceDiscovery(
      ServiceEndpoint serviceEndpoint, Microservices seed) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(opts -> opts.seedMembers(ClusterAddresses.toAddress(seed.discovery().address())));
  }
}
