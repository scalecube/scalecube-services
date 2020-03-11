package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import io.scalecube.services.ServicesProvider;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import reactor.core.publisher.Mono;

/**
 * Default {@link ServicesProvider}.
 */
public class ScaleCubeServicesLifeCycleManager implements ServicesProvider {

  private final Function<Microservices, Collection<ServiceInfo>> serviceFactory;

  // lazy init
  private Collection<ServiceInfo> services;

  /**
   * Create instance from {@link ServiceProvider}.
   *
   * @param serviceProviders old service providers.
   * @return default services provider.
   */
  public static ServicesProvider create(Collection<ServiceProvider> serviceProviders) {
    return new ScaleCubeServicesLifeCycleManager(serviceProviders);
  }

  private ScaleCubeServicesLifeCycleManager(Collection<ServiceProvider> serviceProviders) {
    this.serviceFactory =
        microservices ->
            serviceProviders.stream()
                .map(serviceProvider -> serviceProvider.provide(microservices.call()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.Inject} for inject {@link Microservices},
   * {@link io.scalecube.services.ServiceCall}.
   */
  @Override
  public Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices) {
    this.services =
        this.serviceFactory.apply(microservices).stream()
            .map(service -> Injector.inject(microservices, service))
            .collect(Collectors.toList());
    return Mono.just(this.services);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.AfterConstruct} for initialization service's
   * instance.
   */
  @Override
  public Mono<Void> postConstruct(Microservices microservices) {
    return Mono.fromRunnable(
        () ->
            this.services.forEach(
                service -> Injector.processAfterConstruct(microservices, service)));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.BeforeDestroy} for finilization service's
   * instance.
   */
  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    this.services.forEach(service -> Injector.processBeforeDestroy(microservices, service));
    return Mono.just(microservices);
  }
}
