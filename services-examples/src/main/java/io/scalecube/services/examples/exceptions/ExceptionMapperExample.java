package io.scalecube.services.examples.exceptions;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
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
        new Microservices()
            .errorMapper(new ServiceAProviderErrorMapper()) // default mapper for whole node
            .services(
                ServiceInfo.fromServiceInstance(new ServiceAImpl())
                    .errorMapper(new ServiceAProviderErrorMapper()) // mapper per service instance
                    .build())
            .startAwait();

    System.err.println("ms1 started: " + ms1.serviceAddress());

    Microservices ms2 =
        new Microservices()
            .discovery(options -> options.seeds(ms1.discovery().address()))
            .services(
                call -> {
                  ServiceA serviceA =
                      call.errorMapper(
                              new ServiceAClientErrorMapper()) // service client error mapper
                          .create()
                          .api(ServiceA.class);

                  ServiceB serviceB = new ServiceBImpl(serviceA);

                  return Collections.singleton(ServiceInfo.fromServiceInstance(serviceB).build());
                })
            .startAwait();

    System.err.println("ms2 started: " + ms2.serviceAddress());

    ms2.call()
        .create()
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
