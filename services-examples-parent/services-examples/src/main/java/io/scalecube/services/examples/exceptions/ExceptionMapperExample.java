package io.scalecube.services.examples.exceptions;

import io.scalecube.net.Address;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import reactor.core.publisher.Mono;

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
            .transport(RSocketServiceTransport::new)
            .defaultErrorMapper(new ServiceAProviderErrorMapper()) // default mapper for whole node
            .services(
                ServiceInfo.fromServiceInstance(new ServiceAImpl())
                    .errorMapper(new ServiceAProviderErrorMapper()) // mapper per service instance
                    .build())
            .startAwait();

    System.err.println("ms1 started: " + ms1.serviceAddress());

    final Address address1 = ms1.discovery().address();

    ServiceFactory serviceFactory =
        new ServiceFactory() {
          @Override
          public Collection<ServiceDefinition> getServiceDefinitions() {
            ServiceDefinition serviceA = new ServiceDefinition(ServiceA.class);
            ServiceDefinition serviceB = new ServiceDefinition(ServiceB.class);
            return List.of(serviceA, serviceB);
          }

          @Override
          public Mono<? extends Collection<ServiceInfo>> initializeServices(
              MicroservicesContext microservices) {
            ServiceCall call = microservices.serviceCall();
            ServiceA serviceA =
                call.errorMapper(new ServiceAClientErrorMapper()).api(ServiceA.class);
            ServiceB serviceB = new ServiceBImpl(serviceA);
            ServiceInfo serviceInfoB = ServiceInfo.fromServiceInstance(serviceB).build();
            return Mono.just(Collections.singletonList(serviceInfoB));
          }
        };
    Microservices ms2 =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(address1)))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(serviceFactory)
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
}
