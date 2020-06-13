package io.scalecube.services.examples.helloworld;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.BidiGreetingImpl;
import io.scalecube.services.examples.helloworld.service.api.BidiGreetingService;
import io.scalecube.services.ScalecubeServiceFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import reactor.core.publisher.Flux;

/**
 * The Hello World project is a time-honored tradition in computer programming. It is a simple
 * exercise that gets you started when learning something new. Let’s get started with ScaleCube!
 *
 * <p>the example starts 2 cluster member nodes. 1. seed is a member node and holds no services of
 * its own. 2. The <code>microservices</code> variable is a member that joins seed member and
 * provision <code>BidiGreetingService</code> instance.
 */
public class Example3 {

  /**
   * Start the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // ScaleCube Node node with no members
    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    final Address seedAddress = seed.discovery().address();

    // Create service factory for Bid Greeting Service
    ServiceFactory serviceFactory = ScalecubeServiceFactory.fromInstances(new BidiGreetingImpl());

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices ms =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seedAddress)))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(serviceFactory)
            .startAwait();

    // Create service proxy
    BidiGreetingService service = seed.call().api(BidiGreetingService.class);

    // Execute the services and subscribe to service events
    service
        .greeting(Flux.fromArray(new String[] {"joe", "dan", "roni"}))
        .doOnNext(System.out::println)
        .blockLast();

    seed.onShutdown().block();
    ms.onShutdown().block();
  }
}
