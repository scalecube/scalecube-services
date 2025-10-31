package io.scalecube.services.examples.codecs;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;

public class Example1 {

  public static final String JSON = "application/json";
  public static final String OCTET_STREAM = "application/octet-stream";

  /**
   * Start the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // ScaleCube Node with no members
    Microservices seed =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
            // .defaultContentType(JSON) // set explicit default data format
            );

    final Address seedAddress = seed.discoveryAddress();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices ms =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(seedAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(new GreetingServiceImpl()));

    seed.call()
        .api(GreetingsService.class)
        .sayHello("joe (on JSON dataFormat)")
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

    seed.close();
    ms.close();
  }
}
