package io.scalecube.services.examples.codecs;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Scalecube;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;

public class Example1 {

  public static final String CONTENT_TYPE = "application/protostuff";

  /**
   * Start the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // ScaleCube Node node with no members
    Scalecube seed =
        Scalecube.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .contentType(CONTENT_TYPE) // need to send with non-default data format
            .startAwait();

    final Address seedAddress = seed.discovery().address();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Scalecube ms =
        Scalecube.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seedAddress)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();

    // Create service proxy
    GreetingsService service = seed.call().api(GreetingsService.class);

    // Execute the services and subscribe to service events
    service.sayHello("joe").subscribe(consumer -> System.out.println(consumer.message()));

    seed.onShutdown().block();
    ms.onShutdown().block();
  }
}
