package io.scalecube.services.examples.exceptions;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.mappers.DefaultErrorMapper;
import io.scalecube.services.exceptions.mappers.ServiceClientErrorMapper;
import io.scalecube.services.exceptions.mappers.ServiceProviderErrorMapper;
import java.util.Collections;
import reactor.core.Exceptions;

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
            .defaultErrorMapper(new ServiceAProviderErrorMapper()) // default mapper for whole node
            .services(
                ServiceInfo.fromServiceInstance(new ServiceAImpl())
                    .errorMapper(new ServiceAProviderErrorMapper()) // mapper per service instance
                    .build())
            .startAwait();

    System.err.println("ms1 started: " + ms1.serviceAddress());

    Microservices ms2 =
        Microservices.builder()
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

  private static class ServiceAProviderErrorMapper implements ServiceProviderErrorMapper {

    @Override
    public ServiceMessage toMessage(Throwable throwable) {
      // implement service mapping logic
      if (throwable instanceof ServiceAException) {
        ServiceAException e = (ServiceAException) throwable;
        return ServiceMessage.builder()
            .qualifier(Qualifier.asError(400))
            .data(new ErrorData(e.code(), e.getMessage()))
            .build();
      }

      // or delegate it to default mapper
      return DefaultErrorMapper.INSTANCE.toMessage(throwable);
    }
  }

  private static class ServiceAClientErrorMapper implements ServiceClientErrorMapper {

    @Override
    public Throwable toError(ServiceMessage message) {
      ErrorData data = message.data();

      if (data.getErrorCode() == 42) {
        // implement service mapping logic
        throw Exceptions.propagate(new ServiceAException(data.getErrorMessage()));
      } else {
        // or delegate it to default mapper
        throw Exceptions.propagate(DefaultErrorMapper.INSTANCE.toError(message));
      }
    }
  }
}
