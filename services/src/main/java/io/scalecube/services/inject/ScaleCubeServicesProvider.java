package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import io.scalecube.services.ServicesProvider;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import reactor.core.publisher.Mono;

/** Default {@link ServicesProvider}. */
public class ScaleCubeServicesProvider implements ServicesProvider {

  private final Function<Microservices, Collection<ServiceInfo>> serviceFactory;

  // lazy init
  private Collection<ServiceInfo> services;

  /**
   * Create the instance from {@link ServiceProvider}.
   *
   * @param serviceProviders old service providers.
   * @return default services provider.
   */
  public static ServicesProvider create(Collection<ServiceProvider> serviceProviders) {
    return new ScaleCubeServicesProvider(serviceProviders);
  }

  private ScaleCubeServicesProvider(Collection<ServiceProvider> serviceProviders) {
    this.serviceFactory =
        microservices ->
            serviceProviders.stream()
                .map(serviceProvider -> serviceProvider.provide(microservices.call()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
  }

  /**
   * Since the service instance factory ({@link ServiceProvider}) we have to leave behind does not
   * provide us with information about the types of services produced, there is nothing left for us
   * to do but start creating all the services and then retrieve the type of service, previously
   * saving it as a {@link ScaleCubeServicesProvider} state.
   *
   * <p>{@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.Inject} for inject {@link Microservices},
   * {@link io.scalecube.services.ServiceCall}.
   *
   * @see ServiceInfo
   * @see ServiceDefinition
   */
  @Override
  public Mono<? extends Collection<ServiceDefinition>> provideServiceDefinitions(
      Microservices microservices) {
    this.services =
        this.serviceFactory.apply(microservices).stream()
            .map(service -> Injector.inject(microservices, service))
            .collect(Collectors.toList());
    List<ServiceDefinition> definitions =
        this.services.stream()
            .map(serviceInfo -> new ServiceDefinition(serviceInfo.type(), serviceInfo.tags()))
            .collect(Collectors.toList());
    return Mono.just(definitions);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.AfterConstruct} for initialization service's
   * instance.
   */
  @Override
  public Mono<? extends Collection<ServiceInfo>> provideService(Microservices microservices) {
    return Mono.defer(
        () ->
            Mono.fromCallable(
                () ->
                    this.services.stream()
                        .map(service -> Injector.processAfterConstruct(microservices, service))
                        .collect(Collectors.toList())));
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
