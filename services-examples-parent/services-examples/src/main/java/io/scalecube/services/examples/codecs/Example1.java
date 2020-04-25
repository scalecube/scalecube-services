package io.scalecube.services.examples.codecs;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;

public class Example1 {

  public static final String CONTENT_TYPE = "application/protostuff";
  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance(CONTENT_TYPE);
  private static final DataCodec DATA_CODEC = DataCodec.getInstance(CONTENT_TYPE);

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
            .transport(() -> new RSocketServiceTransport().headersCodec(HEADERS_CODEC))
            .defaultDataEncoder(DATA_CODEC) // need to send with non-default data format
            .startAwait();

    final Address seedAddress = seed.discovery().address();

    // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices ms =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seedAddress)))
            .transport(() -> new RSocketServiceTransport().headersCodec(HEADERS_CODEC))
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
