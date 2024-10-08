package io.scalecube.services.examples.exceptions;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.Collections;

public class ExceptionMapperExample {

  /**
   * Example runner.
   *
   * @param args program arguments.
   * @throws InterruptedException exception.
   */
  public static void main(String[] args) throws InterruptedException {
    Microservices ms1 =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .defaultErrorMapper(
                    new ServiceAProviderErrorMapper()) // default mapper for whole node
                .services(
                    ServiceInfo.fromServiceInstance(new ServiceAImpl())
                        .errorMapper(
                            new ServiceAProviderErrorMapper()) // mapper per service instance
                        .build()));

    System.err.println("ms1 started: " + ms1.serviceAddress());

    final Address address1 = ms1.discoveryAddress();

    Microservices ms2 =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(address1.toString())))
                .transport(RSocketServiceTransport::new)
                .services(
                    call -> {
                      ServiceA serviceA =
                          call.errorMapper(
                                  new ServiceAClientErrorMapper()) // service client error mapper
                              .api(ServiceA.class);

                      ServiceB serviceB = new ServiceBImpl(serviceA);

                      return Collections.singleton(
                          ServiceInfo.fromServiceInstance(serviceB).build());
                    }));

    System.err.println("ms2 started: " + ms2.serviceAddress());

    ms2.call()
        .api(ServiceB.class)
        .doAnotherStuff(0)
        .subscribe(
            System.out::println,
            th ->
                System.err.println(
                    "No service client mapper is defined for ServiceB, "
                        + "so default scalecube mapper is used! -> "
                        + th),
            () -> System.out.println("Completed!"));

    Thread.currentThread().join();
  }
}
