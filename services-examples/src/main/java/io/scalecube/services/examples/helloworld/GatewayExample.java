package io.scalecube.services.examples.helloworld;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.ServiceTransports;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.helloworld.service.api.GreetingsService;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import io.scalecube.transport.Address;

public class GatewayExample {

  /**
   * Example running gateway service (ws,http) and provisioning greeting service.
   *
   * @param args none needed.
   * @throws InterruptedException joining main thread.
   */
  public static void main(String[] args) {
    // 1. ScaleCube Node node with no members
    Microservices gateway =
        Microservices.builder()
            .discovery(serviceEndpoint -> new ScalecubeServiceDiscovery(serviceEndpoint))
            .transport(ServiceTransports::rsocketServiceTransport)
            // expose http(8080) and websocket(8181) via corresponding gateways:
            .gateway(opt -> new WebsocketGateway(opt.id("ws").port(8080)))
            .gateway(opt -> new HttpGateway(opt.id("http").port(8181)).corsEnabled(true))
            .startAwait();

    Address seedAddress =
        Address.create(gateway.discovery().address().host(), gateway.discovery().address().port());

    // 2. Construct a ScaleCube node which joins the cluster hosting the Greeting Service
    Microservices ms =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(seedAddress)))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new GreetingServiceImpl())
            .startAwait();

    // 3. Create service proxy
    GreetingsService service = gateway.call().api(GreetingsService.class);

    // Execute the services and subscribe to service events
    service
        .sayHello("joe")
        .subscribe(
            consumer -> {
              System.out.println(consumer.message());
            });

    gateway.onShutdown().block();
    ms.onShutdown().block();
  }
}
