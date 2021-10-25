package io.scalecube.services.examples.codecs;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;

public class Example1 {

  public static final String JSON = "application/json";
  public static final String PROTOSTUFF = "application/protostuff";
  public static final String OCTET_STREAM = "application/octet-stream";

  /**
   * Start the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // ScaleCube Node node with no members
    Microservices seed =
        Microservices.builder()
            .discovery(
                "seed",
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .defaultContentType(PROTOSTUFF) // set explicit default data format
            .startAwait();

    final Address seedAddress = seed.discovery().address();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices ms =
        Microservices.builder()
            .discovery(
                "ms",
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(cfg -> cfg.seedMembers(seedAddress)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();

    seed.call()
        .api(GreetingsService.class)
        .sayHello("joe (on default dataFormat PROTOSTUFF)")
        .subscribe(consumer -> System.out.println(consumer.message()));

    seed.call()
        .contentType(JSON)
        .api(GreetingsService.class)
        .sayHello("alice (on JSON dataFormat)")
        .subscribe(consumer -> System.out.println(consumer.message()));

    seed.call()
        .contentType(OCTET_STREAM)
        .api(GreetingsService.class)
        .sayHello("bob (on java native Serializable/Externalizable dataFormat)")
        .subscribe(consumer -> System.out.println(consumer.message()));

    seed.onShutdown().block();
    ms.onShutdown().block();
  }
}
