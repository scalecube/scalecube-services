package io.scalecube.services.routings;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.routings.sut.CanaryService;
import io.scalecube.services.routings.sut.GreetingServiceImplA;
import io.scalecube.services.routings.sut.GreetingServiceImplB;
import io.scalecube.services.routings.sut.WeightedRandomRouter;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import reactor.core.publisher.Mono;

public class ServiceTagsExample {

  /**
   * Main runner.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    Microservices gateway =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Address seedAddress = gateway.discoveryAddress();

    Microservices services1 =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(cfg -> cfg.seedMembers(seedAddress.toString())))
            .transport(RSocketServiceTransport::new)
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImplA())
                    .tag("Weight", "0.3")
                    .build())
            .startAwait();

    Microservices services2 =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(cfg -> cfg.seedMembers(seedAddress.toString())))
            .transport(RSocketServiceTransport::new)
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImplB())
                    .tag("Weight", "0.7")
                    .build())
            .startAwait();

    CanaryService service =
        gateway.call().router(WeightedRandomRouter.class).api(CanaryService.class);

    for (int i = 0; i < 10; i++) {
      Mono.from(service.greeting(new GreetingRequest("joe")))
          .doOnNext(
              success -> {
                success.getResult().startsWith("B");
                System.out.println(success);
              });
    }
  }
}
