package io.scalecube.services.examples.exceptions;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketTransportResources;
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
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .setupTransport(RSocketTransportResources::new)
            .transportFactory(RSocketServiceTransportFactory::new)
            .defaultErrorMapper(new ServiceAProviderErrorMapper()) // default mapper for whole node
            .services(
                ServiceInfo.fromServiceInstance(new ServiceAImpl())
                    .errorMapper(new ServiceAProviderErrorMapper()) // mapper per service instance
                    .build())
            .startAwait();

    System.err.println("ms1 started: " + ms1.serviceAddress());

    Microservices ms2 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, ms1))
            .setupTransport(RSocketTransportResources::new)
            .transportFactory(RSocketServiceTransportFactory::new)
            .services(
                call -> {
                  ServiceA serviceA =
                      call.errorMapper(
                          new ServiceAClientErrorMapper()) // service client error mapper
                          .api(ServiceA.class);

                  ServiceB serviceB = new ServiceBImpl(serviceA);

                  return Collections.singleton(ServiceInfo.fromServiceInstance(serviceB).build());
                })
            .startAwait();

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

  private static ServiceDiscovery serviceDiscovery(
      ServiceEndpoint serviceEndpoint, Microservices ms1) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(opts -> opts.seedMembers(ms1.discovery().address()));
  }
}
