package io.scalecube.services.inject;

import static java.util.Arrays.asList;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import reactor.core.publisher.Mono;

public class ScaleCubeServicesProvider implements ServicesProvider {

  private final Collection<?> services;

  public static ScaleCubeServicesProvider from(Object... services) {
    return from(services == null ? Collections.emptyList() : asList(services));
  }

  public static ScaleCubeServicesProvider from(Collection<?> services) {
    return new ScaleCubeServicesProvider(services);
  }

  private ScaleCubeServicesProvider(Collection<?> services) {
    this.services = services == null ? Collections.emptyList() : services;
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices) {
    List<ServiceInfo> services =
        this.services.stream()
            .map(
                service -> {
                  Object serviceInstance;
                  ServiceInfo serviceInfo;
                  if (service instanceof ServiceInfo) {
                    serviceInfo = (ServiceInfo) service;
                    serviceInstance = serviceInfo.serviceInstance();
                  } else {
                    serviceInstance = service;
                    serviceInfo = ServiceInfo.fromServiceInstance(serviceInstance).build();
                  }
                  Injector.inject(microservices, serviceInstance);
                  return serviceInfo;
                })
            .collect(Collectors.toList());
    return Mono.just(services);
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    Stream.of(this.services)
        .forEach(service -> Injector.processBeforeDestroy(microservices, service));
    return Mono.just(microservices);
  }
}
