package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesLifeCycleManager;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Mono;

public class ScaleCubeServicesLifeCycleManager implements ServicesLifeCycleManager {

  private final Collection<?> services;

  public static ScaleCubeServicesLifeCycleManager from(Object... services) {
    return from(services == null ? Collections.emptyList() : Arrays.asList(services));
  }

  public static ScaleCubeServicesLifeCycleManager from(Collection<?> services) {
    return new ScaleCubeServicesLifeCycleManager(services);
  }

  private ScaleCubeServicesLifeCycleManager(Collection<?> services) {
    this.services =
        services == null
            ? Collections.emptyList()
            : services.stream()
                .map(
                    service ->
                        service instanceof ServiceInfo
                            ? ((ServiceInfo) service).serviceInstance()
                            : service)
                .collect(Collectors.toList());
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices) {
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
  public Mono<Void> postConstruct(Microservices microservices) {
    return Mono.fromRunnable(
        () ->
            this.services.forEach(
                service -> Injector.processAfterConstruct(microservices, service)));
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    this.services.forEach(service -> Injector.processBeforeDestroy(microservices, service));
    return Mono.just(microservices);
  }
}
