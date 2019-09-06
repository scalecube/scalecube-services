package io.scalecube.services.examples.exceptions;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionMapperExample {

  public static final Logger LOGGER = LoggerFactory.getLogger(ExceptionMapperExample.class);

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
                    .requestMapper(
                        message -> {
                          LOGGER.info("REQ ServiceAImpl >>>> {}", message);
                          return message;
                        })
                    .responseMapper(
                        message -> {
                          LOGGER.info("RESP ServiceAImpl <<<< {}", message);
                          return message;
                        })
                    .errorHandler(
                        (message, throwable) ->
                            LOGGER.error(
                                "ERROR ServiceAImpl occurred: [{}] for request: {}",
                                throwable,
                                message))
                    .errorMapper(new ServiceAProviderErrorMapper()) // mapper per service instance
                    .build())
            .startAwait();

    System.err.println("ms1 started: " + ms1.serviceAddress());

    Microservices ms2 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, ms1))
            .transport(RSocketServiceTransport::new)
            .services(
                serviceCall -> {
                  ServiceA serviceA =
                      serviceCall
                          .requestMapper(
                              message -> {
                                LOGGER.info("REQ api(ServiceA) >>>> {}", message);
                                return message;
                              })
                          .responseMapper(
                              message -> {
                                LOGGER.info("RESP api(ServiceA) <<<< {}", message);
                                return message;
                              })
                          .errorHandler(
                              (message, throwable) ->
                                  LOGGER.error(
                                      "ERROR api(ServiceA) occurred: [{}] for request: {}",
                                      throwable,
                                      message))
                          .errorMapper(
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
        .options(opts -> opts.membership(cfg -> cfg.seedMembers(ms1.discovery().address())));
  }
}
